use std::sync::Arc;

use ahash::{AHashMap, AHashSet};
use arrow::array::{Array, RecordBatch, StructArray};
use arrow::compute;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::Result as DFResult;
use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};

use crate::error::{NanoError, Result};
use crate::ir::*;
use crate::query::ast::{AggFunc, CompOp, Literal};
use crate::store::graph::GraphStorage;
use tracing::{debug, info, instrument};

use super::node_scan::{NodeScanExec, NodeScanPredicate};
use super::physical::ExpandExec;

type ScanProjectionMap = AHashMap<String, Option<AHashSet<String>>>;

/// Execute a query IR against a graph storage, returning a RecordBatch of results.
#[instrument(skip(ir, storage, params), fields(query = %ir.name, pipeline_len = ir.pipeline.len()))]
pub async fn execute_query(
    ir: &QueryIR,
    storage: Arc<GraphStorage>,
    params: &ParamMap,
) -> Result<Vec<RecordBatch>> {
    info!("executing query");
    let has_aggregation = ir
        .return_exprs
        .iter()
        .any(|p| matches!(&p.expr, IRExpr::Aggregate { .. }));
    let single_scan_pushdown = analyze_single_scan_pushdown(&ir.pipeline, params);
    let scan_limit_pushdown = if !has_aggregation && ir.order_by.is_empty() {
        ir.limit.and_then(|v| {
            single_scan_pushdown
                .as_ref()
                .filter(|info| info.all_filters_pushdown)
                .map(|_| v as usize)
        })
    } else {
        None
    };
    let scan_projections = analyze_scan_projection_requirements(ir);
    // Build the physical plan from IR
    let plan = build_physical_plan(
        &ir.pipeline,
        storage.clone(),
        params,
        scan_limit_pushdown,
        &scan_projections,
    )?;

    // Execute the plan to get intermediate results
    let ctx = SessionContext::new();
    let task_ctx = ctx.task_ctx();
    let stream = plan
        .execute(0, task_ctx)
        .map_err(|e| NanoError::Execution(e.to_string()))?;

    let can_stream_batchwise = !has_aggregation && ir.order_by.is_empty() && ir.limit.is_none();
    debug!(
        has_aggregation,
        has_order = !ir.order_by.is_empty(),
        has_limit = ir.limit.is_some(),
        fast_path = can_stream_batchwise,
        "query execution mode selected"
    );

    if can_stream_batchwise {
        use futures::StreamExt;
        let mut out = Vec::new();
        let mut stream = stream;
        while let Some(batch) = stream.next().await {
            let batch = batch.map_err(|e| NanoError::Execution(e.to_string()))?;
            if batch.num_rows() == 0 {
                continue;
            }
            let filtered = apply_ir_filters(&ir.pipeline, &[batch], params)?;
            if filtered.is_empty() {
                continue;
            }
            let projected = apply_projection(&ir.return_exprs, &filtered, params)?;
            out.extend(projected.into_iter().filter(|b| b.num_rows() > 0));
        }
        info!(result_batches = out.len(), "query execution complete");
        return Ok(out);
    }

    let batches: Vec<RecordBatch> = {
        use futures::StreamExt;
        let mut batches = Vec::new();
        let mut stream = stream;
        while let Some(batch) = stream.next().await {
            let b = batch.map_err(|e| NanoError::Execution(e.to_string()))?;
            batches.push(b);
        }
        batches
    };

    if batches.is_empty() || (batches.len() == 1 && batches[0].num_rows() == 0) {
        return Ok(vec![]);
    }

    // Apply filters from the pipeline that reference struct fields
    let filtered = apply_ir_filters(&ir.pipeline, &batches, params)?;

    // Apply return projections
    if has_aggregation {
        let result = apply_aggregation(&ir.return_exprs, &filtered, params)?;
        let result = apply_order_and_limit(&result, &ir.order_by, ir.limit)?;
        info!(result_batches = result.len(), "query execution complete");
        Ok(result)
    } else {
        let projected = apply_projection(&ir.return_exprs, &filtered, params)?;
        let result = apply_order_and_limit(&projected, &ir.order_by, ir.limit)?;
        info!(result_batches = result.len(), "query execution complete");
        Ok(result)
    }
}

fn build_physical_plan(
    pipeline: &[IROp],
    storage: Arc<GraphStorage>,
    params: &ParamMap,
    scan_limit_pushdown: Option<usize>,
    scan_projections: &ScanProjectionMap,
) -> Result<Arc<dyn ExecutionPlan>> {
    let mut current_plan: Option<Arc<dyn ExecutionPlan>> = None;

    for op in pipeline {
        match op {
            IROp::NodeScan {
                variable,
                type_name,
                filters,
            } => {
                let node_schema =
                    storage.catalog.node_types.get(type_name).ok_or_else(|| {
                        NanoError::Plan(format!("unknown node type: {}", type_name))
                    })?;

                // Build struct output schema, pruning to required fields when possible.
                let struct_fields = select_scan_struct_fields(
                    variable,
                    &node_schema.arrow_schema,
                    scan_projections,
                );
                let struct_field =
                    Field::new(variable, DataType::Struct(struct_fields.into()), false);

                let output_schema = if let Some(ref plan) = current_plan {
                    // Append to existing schema
                    let mut fields: Vec<Field> = plan
                        .schema()
                        .fields()
                        .iter()
                        .map(|f| f.as_ref().clone())
                        .collect();
                    fields.push(struct_field);
                    Arc::new(Schema::new(fields))
                } else {
                    Arc::new(Schema::new(vec![struct_field]))
                };

                let mut pushdown_filters = build_scan_pushdown_filters(variable, filters, params);
                pushdown_filters
                    .extend(build_explicit_pushdown_filters(variable, pipeline, params));

                let scan = NodeScanExec::new(
                    type_name.clone(),
                    variable.clone(),
                    Arc::new(Schema::new(vec![
                        output_schema
                            .field(output_schema.fields().len() - 1)
                            .as_ref()
                            .clone(),
                    ])),
                    pushdown_filters,
                    if current_plan.is_none() {
                        scan_limit_pushdown
                    } else {
                        None
                    },
                    storage.clone(),
                );

                if let Some(prev) = current_plan {
                    // Cross join with previous plan (for multi-binding patterns)
                    current_plan = Some(Arc::new(CrossJoinExec::new(
                        prev,
                        Arc::new(scan),
                        output_schema,
                    )));
                } else {
                    current_plan = Some(Arc::new(scan));
                }
            }
            IROp::Expand {
                src_var,
                dst_var,
                edge_type,
                direction,
                dst_type,
            } => {
                let input = current_plan
                    .ok_or_else(|| NanoError::Plan("Expand without input".to_string()))?;
                let expand = ExpandExec::new(
                    input,
                    src_var.clone(),
                    dst_var.clone(),
                    edge_type.clone(),
                    *direction,
                    dst_type.clone(),
                    storage.clone(),
                );
                current_plan = Some(Arc::new(expand));
            }
            IROp::Filter(_) => {
                // Filters are applied post-execution for simplicity in v0
            }
            IROp::AntiJoin { outer_var, inner } => {
                let outer = current_plan
                    .ok_or_else(|| NanoError::Plan("AntiJoin without outer input".to_string()))?;

                // Build the inner plan, seeding it with the outer plan as input
                // so Expand ops have the source rows to work with
                let inner_plan = build_physical_plan_with_input(
                    inner,
                    storage.clone(),
                    outer.clone(),
                    params,
                    scan_projections,
                )?;

                current_plan = Some(Arc::new(AntiJoinExec::new(
                    outer,
                    inner_plan,
                    outer_var.clone(),
                    inner.clone(),
                    params.clone(),
                    storage.clone(),
                )));
            }
        }
    }

    current_plan.ok_or_else(|| NanoError::Plan("empty pipeline".to_string()))
}

/// Like build_physical_plan but seeds with an initial input plan.
/// Used for AntiJoin inner pipelines that start with Expand (needing source rows).
fn build_physical_plan_with_input(
    pipeline: &[IROp],
    storage: Arc<GraphStorage>,
    input: Arc<dyn ExecutionPlan>,
    params: &ParamMap,
    scan_projections: &ScanProjectionMap,
) -> Result<Arc<dyn ExecutionPlan>> {
    let mut current_plan: Option<Arc<dyn ExecutionPlan>> = Some(input);

    for op in pipeline {
        match op {
            IROp::NodeScan {
                variable,
                type_name,
                filters,
            } => {
                let node_schema =
                    storage.catalog.node_types.get(type_name).ok_or_else(|| {
                        NanoError::Plan(format!("unknown node type: {}", type_name))
                    })?;

                let struct_fields = select_scan_struct_fields(
                    variable,
                    &node_schema.arrow_schema,
                    scan_projections,
                );
                let struct_field =
                    Field::new(variable, DataType::Struct(struct_fields.into()), false);

                let output_schema = if let Some(ref plan) = current_plan {
                    let mut fields: Vec<Field> = plan
                        .schema()
                        .fields()
                        .iter()
                        .map(|f| f.as_ref().clone())
                        .collect();
                    fields.push(struct_field);
                    Arc::new(Schema::new(fields))
                } else {
                    Arc::new(Schema::new(vec![struct_field]))
                };

                let mut pushdown_filters = build_scan_pushdown_filters(variable, filters, params);
                pushdown_filters
                    .extend(build_explicit_pushdown_filters(variable, pipeline, params));

                let scan = NodeScanExec::new(
                    type_name.clone(),
                    variable.clone(),
                    Arc::new(Schema::new(vec![
                        output_schema
                            .field(output_schema.fields().len() - 1)
                            .as_ref()
                            .clone(),
                    ])),
                    pushdown_filters,
                    None,
                    storage.clone(),
                );

                if let Some(prev) = current_plan {
                    current_plan = Some(Arc::new(CrossJoinExec::new(
                        prev,
                        Arc::new(scan),
                        output_schema,
                    )));
                } else {
                    current_plan = Some(Arc::new(scan));
                }
            }
            IROp::Expand {
                src_var,
                dst_var,
                edge_type,
                direction,
                dst_type,
            } => {
                let input = current_plan
                    .ok_or_else(|| NanoError::Plan("Expand without input".to_string()))?;
                let expand = ExpandExec::new(
                    input,
                    src_var.clone(),
                    dst_var.clone(),
                    edge_type.clone(),
                    *direction,
                    dst_type.clone(),
                    storage.clone(),
                );
                current_plan = Some(Arc::new(expand));
            }
            IROp::Filter(_) => {
                // Filters handled post-execution
            }
            IROp::AntiJoin { outer_var, inner } => {
                let outer = current_plan
                    .ok_or_else(|| NanoError::Plan("AntiJoin without outer input".to_string()))?;
                let inner_plan = build_physical_plan_with_input(
                    inner,
                    storage.clone(),
                    outer.clone(),
                    params,
                    scan_projections,
                )?;
                current_plan = Some(Arc::new(AntiJoinExec::new(
                    outer,
                    inner_plan,
                    outer_var.clone(),
                    inner.clone(),
                    params.clone(),
                    storage.clone(),
                )));
            }
        }
    }

    current_plan.ok_or_else(|| NanoError::Plan("empty inner pipeline".to_string()))
}

fn build_scan_pushdown_filters(
    variable: &str,
    filters: &[IRFilter],
    params: &ParamMap,
) -> Vec<NodeScanPredicate> {
    filters
        .iter()
        .filter_map(|filter| pushdown_scan_filter(variable, filter, params))
        .collect()
}

fn build_explicit_pushdown_filters(
    variable: &str,
    pipeline: &[IROp],
    params: &ParamMap,
) -> Vec<NodeScanPredicate> {
    pipeline
        .iter()
        .filter_map(|op| match op {
            IROp::Filter(filter) => pushdown_scan_filter(variable, filter, params),
            _ => None,
        })
        .collect()
}

#[derive(Debug, Clone)]
struct SingleScanPushdownInfo {
    all_filters_pushdown: bool,
}

fn analyze_single_scan_pushdown(
    pipeline: &[IROp],
    params: &ParamMap,
) -> Option<SingleScanPushdownInfo> {
    let mut scan_var: Option<&str> = None;
    let mut scan_filters: &[IRFilter] = &[];
    let mut explicit_filters: Vec<&IRFilter> = Vec::new();

    for op in pipeline {
        match op {
            IROp::NodeScan {
                variable, filters, ..
            } => {
                if scan_var.is_some() {
                    return None;
                }
                scan_var = Some(variable);
                scan_filters = filters;
            }
            IROp::Filter(filter) => explicit_filters.push(filter),
            _ => return None,
        }
    }

    let variable = scan_var?;
    let mut all_filters_pushdown = true;
    for filter in scan_filters {
        if pushdown_scan_filter(variable, filter, params).is_none() {
            all_filters_pushdown = false;
        }
    }

    for filter in explicit_filters {
        if pushdown_scan_filter(variable, filter, params).is_none() {
            all_filters_pushdown = false;
        }
    }

    Some(SingleScanPushdownInfo {
        all_filters_pushdown,
    })
}

fn analyze_scan_projection_requirements(ir: &QueryIR) -> ScanProjectionMap {
    let mut scan_variables = AHashSet::new();
    collect_scan_variables(&ir.pipeline, &mut scan_variables);

    let mut requirements: ScanProjectionMap = scan_variables
        .iter()
        .map(|var| (var.clone(), Some(AHashSet::new())))
        .collect();

    collect_pipeline_projection_requirements(&ir.pipeline, &scan_variables, &mut requirements);
    for proj in &ir.return_exprs {
        collect_expr_projection_requirements(&proj.expr, &scan_variables, &mut requirements);
    }
    for ordering in &ir.order_by {
        collect_expr_projection_requirements(&ordering.expr, &scan_variables, &mut requirements);
    }

    // Keep id available for join/expand semantics and stable downstream behavior.
    for required in requirements.values_mut() {
        if let Some(props) = required {
            props.insert("id".to_string());
        }
    }

    requirements
}

fn collect_scan_variables(pipeline: &[IROp], out: &mut AHashSet<String>) {
    for op in pipeline {
        match op {
            IROp::NodeScan { variable, .. } => {
                out.insert(variable.clone());
            }
            IROp::AntiJoin { inner, .. } => collect_scan_variables(inner, out),
            _ => {}
        }
    }
}

fn collect_pipeline_projection_requirements(
    pipeline: &[IROp],
    scan_variables: &AHashSet<String>,
    requirements: &mut ScanProjectionMap,
) {
    for op in pipeline {
        match op {
            IROp::NodeScan {
                variable, filters, ..
            } => {
                for filter in filters {
                    collect_expr_projection_requirements(
                        &filter.left,
                        scan_variables,
                        requirements,
                    );
                    collect_expr_projection_requirements(
                        &filter.right,
                        scan_variables,
                        requirements,
                    );
                }

                // Always keep id available for the bound variable.
                mark_scan_property(requirements, variable, "id");
            }
            IROp::Expand { src_var, .. } => {
                mark_scan_property(requirements, src_var, "id");
            }
            IROp::Filter(filter) => {
                collect_expr_projection_requirements(&filter.left, scan_variables, requirements);
                collect_expr_projection_requirements(&filter.right, scan_variables, requirements);
            }
            IROp::AntiJoin { outer_var, inner } => {
                mark_scan_property(requirements, outer_var, "id");
                collect_pipeline_projection_requirements(inner, scan_variables, requirements);
            }
        }
    }
}

fn collect_expr_projection_requirements(
    expr: &IRExpr,
    scan_variables: &AHashSet<String>,
    requirements: &mut ScanProjectionMap,
) {
    match expr {
        IRExpr::PropAccess { variable, property } => {
            if scan_variables.contains(variable) {
                mark_scan_property(requirements, variable, property);
            }
        }
        IRExpr::Variable(variable) => {
            if scan_variables.contains(variable) {
                mark_scan_full(requirements, variable);
            }
        }
        IRExpr::Aggregate { arg, .. } => {
            collect_expr_projection_requirements(arg, scan_variables, requirements);
        }
        _ => {}
    }
}

fn mark_scan_property(requirements: &mut ScanProjectionMap, variable: &str, property: &str) {
    if let Some(required) = requirements.get_mut(variable) {
        if let Some(props) = required {
            props.insert(property.to_string());
        }
    }
}

fn mark_scan_full(requirements: &mut ScanProjectionMap, variable: &str) {
    if let Some(required) = requirements.get_mut(variable) {
        *required = None;
    }
}

fn select_scan_struct_fields(
    variable: &str,
    node_schema: &SchemaRef,
    scan_projections: &ScanProjectionMap,
) -> Vec<Field> {
    match scan_projections.get(variable) {
        Some(Some(required_props)) => {
            let selected: Vec<Field> = node_schema
                .fields()
                .iter()
                .filter(|field| required_props.contains(field.name()))
                .map(|f| f.as_ref().clone())
                .collect();
            if selected.is_empty() {
                node_schema
                    .fields()
                    .iter()
                    .map(|f| f.as_ref().clone())
                    .collect()
            } else {
                selected
            }
        }
        _ => node_schema
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect(),
    }
}

fn pushdown_scan_filter(
    variable: &str,
    filter: &IRFilter,
    params: &ParamMap,
) -> Option<NodeScanPredicate> {
    match (&filter.left, &filter.right) {
        (
            IRExpr::PropAccess {
                variable: v,
                property,
            },
            rhs,
        ) if v == variable => pushdown_literal(rhs, params).map(|literal| NodeScanPredicate {
            property: property.clone(),
            op: filter.op,
            literal,
        }),
        (
            lhs,
            IRExpr::PropAccess {
                variable: v,
                property,
            },
        ) if v == variable => pushdown_literal(lhs, params).map(|literal| NodeScanPredicate {
            property: property.clone(),
            op: flip_comp_op(filter.op),
            literal,
        }),
        _ => None,
    }
}

fn pushdown_literal(expr: &IRExpr, params: &ParamMap) -> Option<Literal> {
    match expr {
        IRExpr::Literal(lit) => Some(lit.clone()),
        IRExpr::Param(name) => params.get(name).cloned(),
        _ => None,
    }
}

fn flip_comp_op(op: CompOp) -> CompOp {
    match op {
        CompOp::Eq => CompOp::Eq,
        CompOp::Ne => CompOp::Ne,
        CompOp::Gt => CompOp::Lt,
        CompOp::Lt => CompOp::Gt,
        CompOp::Ge => CompOp::Le,
        CompOp::Le => CompOp::Ge,
    }
}

fn apply_ir_filters(
    pipeline: &[IROp],
    batches: &[RecordBatch],
    params: &ParamMap,
) -> Result<Vec<RecordBatch>> {
    let scan_variables = pipeline_scan_variables(pipeline);
    let mut filters: Vec<&IRFilter> = Vec::new();

    for op in pipeline {
        match op {
            IROp::Filter(f) => {
                if is_explicit_filter_pushed_down(&scan_variables, f, params) {
                    continue;
                }
                filters.push(f);
            }
            IROp::NodeScan {
                variable,
                filters: scan_filters,
                ..
            } => {
                for f in scan_filters {
                    if pushdown_scan_filter(variable, f, params).is_none() {
                        filters.push(f);
                    }
                }
            }
            _ => {}
        }
    }

    if filters.is_empty() {
        return Ok(batches.to_vec());
    }

    let mut result = batches.to_vec();
    for filter in filters {
        result = apply_single_filter(filter, &result, params)?;
    }
    Ok(result)
}

fn pipeline_scan_variables(pipeline: &[IROp]) -> AHashSet<String> {
    pipeline
        .iter()
        .filter_map(|op| match op {
            IROp::NodeScan { variable, .. } => Some(variable.clone()),
            _ => None,
        })
        .collect()
}

fn is_explicit_filter_pushed_down(
    scan_variables: &AHashSet<String>,
    filter: &IRFilter,
    params: &ParamMap,
) -> bool {
    scan_variables
        .iter()
        .any(|variable| pushdown_scan_filter(variable, filter, params).is_some())
}

fn apply_single_filter(
    filter: &IRFilter,
    batches: &[RecordBatch],
    params: &ParamMap,
) -> Result<Vec<RecordBatch>> {
    let mut result = Vec::new();
    for batch in batches {
        let left = eval_ir_expr(&filter.left, batch, params)?;
        let right = eval_ir_expr(&filter.right, batch, params)?;

        // Cast right to match left's data type if they differ
        let right = if left.data_type() != right.data_type() {
            arrow::compute::cast(&right, left.data_type())
                .map_err(|e| NanoError::Execution(format!("cast error: {}", e)))?
        } else {
            right
        };

        let mask = compare_arrays(&left, &right, filter.op)?;

        let filtered = arrow::compute::filter_record_batch(batch, &mask)
            .map_err(|e| NanoError::Execution(e.to_string()))?;
        if filtered.num_rows() > 0 {
            result.push(filtered);
        }
    }
    Ok(result)
}

fn eval_ir_expr(
    expr: &IRExpr,
    batch: &RecordBatch,
    params: &ParamMap,
) -> Result<arrow::array::ArrayRef> {
    match expr {
        IRExpr::PropAccess { variable, property } => {
            let col_idx = batch.schema().index_of(variable).map_err(|e| {
                NanoError::Execution(format!("column {} not found: {}", variable, e))
            })?;
            let col = batch.column(col_idx);
            let struct_arr = col.as_any().downcast_ref::<StructArray>().ok_or_else(|| {
                NanoError::Execution(format!("column {} is not a struct", variable))
            })?;
            let prop_col = struct_arr.column_by_name(property).ok_or_else(|| {
                NanoError::Execution(format!("struct {} has no field {}", variable, property))
            })?;
            Ok(prop_col.clone())
        }
        IRExpr::Literal(lit) => {
            let num_rows = batch.num_rows();
            Ok(literal_to_array(lit, num_rows))
        }
        IRExpr::Variable(name) => {
            let col_idx = batch
                .schema()
                .index_of(name)
                .map_err(|e| NanoError::Execution(format!("variable {} not found: {}", name, e)))?;
            Ok(batch.column(col_idx).clone())
        }
        IRExpr::Param(name) => {
            let lit = params
                .get(name)
                .ok_or_else(|| NanoError::Execution(format!("parameter ${} not provided", name)))?;
            Ok(literal_to_array(lit, batch.num_rows()))
        }
        _ => Err(NanoError::Execution(
            "unsupported expr in filter".to_string(),
        )),
    }
}

fn literal_to_array(lit: &Literal, num_rows: usize) -> arrow::array::ArrayRef {
    match lit {
        Literal::String(s) => Arc::new(arrow::array::StringArray::from(vec![s.as_str(); num_rows])),
        Literal::Integer(n) => Arc::new(arrow::array::Int64Array::from(vec![*n; num_rows])),
        Literal::Float(f) => Arc::new(arrow::array::Float64Array::from(vec![*f; num_rows])),
        Literal::Bool(b) => Arc::new(arrow::array::BooleanArray::from(vec![*b; num_rows])),
    }
}

fn compare_arrays(
    left: &arrow::array::ArrayRef,
    right: &arrow::array::ArrayRef,
    op: CompOp,
) -> Result<arrow::array::BooleanArray> {
    use arrow::compute::kernels::cmp;
    let result = match op {
        CompOp::Eq => cmp::eq(left, right),
        CompOp::Ne => cmp::neq(left, right),
        CompOp::Gt => cmp::gt(left, right),
        CompOp::Lt => cmp::lt(left, right),
        CompOp::Ge => cmp::gt_eq(left, right),
        CompOp::Le => cmp::lt_eq(left, right),
    }
    .map_err(|e| NanoError::Execution(format!("comparison error: {}", e)))?;
    Ok(result)
}

fn apply_projection(
    projections: &[IRProjection],
    batches: &[RecordBatch],
    params: &ParamMap,
) -> Result<Vec<RecordBatch>> {
    if batches.is_empty() {
        return Ok(vec![]);
    }

    // Build output schema from projections
    let sample = &batches[0];
    let mut out_fields = Vec::new();
    for proj in projections {
        let (name, dt, nullable) =
            infer_projection_field(&proj.expr, proj.alias.as_deref(), sample)?;
        out_fields.push(Field::new(name, dt, nullable));
    }
    let out_schema = Arc::new(Schema::new(out_fields));

    let mut result = Vec::new();
    for batch in batches {
        let mut columns = Vec::new();
        for proj in projections {
            let col = eval_ir_expr(&proj.expr, batch, params)?;
            columns.push(col);
        }
        let out_batch = RecordBatch::try_new(out_schema.clone(), columns)
            .map_err(|e| NanoError::Execution(format!("projection error: {}", e)))?;
        result.push(out_batch);
    }
    Ok(result)
}

fn infer_projection_field(
    expr: &IRExpr,
    alias: Option<&str>,
    batch: &RecordBatch,
) -> Result<(String, DataType, bool)> {
    match expr {
        IRExpr::PropAccess { variable, property } => {
            let col_idx = batch
                .schema()
                .index_of(variable)
                .map_err(|e| NanoError::Execution(e.to_string()))?;
            let col = batch.column(col_idx);
            let struct_arr = col
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| NanoError::Execution("not a struct".to_string()))?;
            let field = struct_arr
                .fields()
                .iter()
                .find(|f| f.name() == property)
                .ok_or_else(|| NanoError::Execution(format!("field {} not found", property)))?;
            let name = alias.unwrap_or(property).to_string();
            Ok((name, field.data_type().clone(), field.is_nullable()))
        }
        IRExpr::Literal(lit) => {
            let name = alias.unwrap_or("literal").to_string();
            let dt = match lit {
                Literal::String(_) => DataType::Utf8,
                Literal::Integer(_) => DataType::Int64,
                Literal::Float(_) => DataType::Float64,
                Literal::Bool(_) => DataType::Boolean,
            };
            Ok((name, dt, false))
        }
        IRExpr::Variable(v) => {
            let name = alias.unwrap_or(v).to_string();
            Ok((name, DataType::Utf8, true))
        }
        IRExpr::Param(p) => {
            let name = alias.unwrap_or(p).to_string();
            Ok((name, DataType::Utf8, true))
        }
        IRExpr::AliasRef(a) => {
            let name = alias.unwrap_or(a).to_string();
            Ok((name, DataType::Int64, true))
        }
        IRExpr::Aggregate { func, arg } => {
            let name = alias.unwrap_or(&func.to_string()).to_string();
            let dt = match func {
                AggFunc::Count => DataType::Int64,
                AggFunc::Avg => DataType::Float64,
                _ => {
                    // Try to infer from arg
                    let (_, dt, _) = infer_projection_field(arg, None, batch)?;
                    dt
                }
            };
            Ok((name, dt, true))
        }
    }
}

fn apply_aggregation(
    projections: &[IRProjection],
    batches: &[RecordBatch],
    params: &ParamMap,
) -> Result<Vec<RecordBatch>> {
    if batches.is_empty() {
        return Ok(vec![]);
    }

    // Concatenate all batches
    let schema = batches[0].schema();
    let combined = if batches.len() == 1 {
        batches[0].clone()
    } else {
        compute::concat_batches(&schema, batches)
            .map_err(|e| NanoError::Execution(e.to_string()))?
    };

    // Identify group-by and aggregate expressions
    let mut group_exprs: Vec<(usize, &IRProjection)> = Vec::new();
    let mut agg_exprs: Vec<(usize, &IRProjection)> = Vec::new();

    for (i, proj) in projections.iter().enumerate() {
        match &proj.expr {
            IRExpr::Aggregate { .. } => agg_exprs.push((i, proj)),
            _ => group_exprs.push((i, proj)),
        }
    }

    // Evaluate group keys
    let mut group_columns: Vec<arrow::array::ArrayRef> = Vec::new();
    for (_, proj) in &group_exprs {
        let col = eval_ir_expr(&proj.expr, &combined, params)?;
        group_columns.push(col);
    }

    // Simple grouping: build a hashmap of group key -> row indices
    let num_rows = combined.num_rows();
    let mut groups: AHashMap<Vec<String>, Vec<usize>> = AHashMap::new();

    for row in 0..num_rows {
        let mut key = Vec::new();
        for gc in &group_columns {
            key.push(array_value_to_string(gc, row));
        }
        groups.entry(key).or_default().push(row);
    }

    // Build output
    let mut output_fields = Vec::new();
    let mut output_columns: Vec<Vec<String>> = Vec::new();
    let mut output_agg_columns: Vec<Vec<f64>> = Vec::new();

    // Initialize output column storage
    for (_, proj) in &group_exprs {
        let (name, dt, nullable) =
            infer_projection_field(&proj.expr, proj.alias.as_deref(), &combined)?;
        output_fields.push(Field::new(name, dt, nullable));
        output_columns.push(Vec::new());
    }
    for (_, proj) in &agg_exprs {
        let (name, dt, nullable) =
            infer_projection_field(&proj.expr, proj.alias.as_deref(), &combined)?;
        output_fields.push(Field::new(name, dt, nullable));
        output_agg_columns.push(Vec::new());
    }

    // Compute groups
    let mut group_keys_ordered: Vec<Vec<String>> = groups.keys().cloned().collect();
    group_keys_ordered.sort();

    for key in &group_keys_ordered {
        let rows = &groups[key];

        // Group columns
        for (col_idx, _) in group_exprs.iter().enumerate() {
            output_columns[col_idx].push(key[col_idx].clone());
        }

        // Aggregate columns
        for (agg_idx, (_, proj)) in agg_exprs.iter().enumerate() {
            if let IRExpr::Aggregate { func, arg } = &proj.expr {
                let arg_col = eval_ir_expr(arg, &combined, params)?;
                let value = compute_aggregate(func, &arg_col, rows)?;
                output_agg_columns[agg_idx].push(value);
            }
        }
    }

    // Convert to RecordBatch
    let out_schema = Arc::new(Schema::new(output_fields.clone()));
    let mut arrays: Vec<arrow::array::ArrayRef> = Vec::new();

    for (col_idx, (_, proj)) in group_exprs.iter().enumerate() {
        let (_, dt, _) = infer_projection_field(&proj.expr, proj.alias.as_deref(), &combined)?;
        let arr = strings_to_array(&output_columns[col_idx], &dt);
        arrays.push(arr);
    }

    for (agg_idx, (_, proj)) in agg_exprs.iter().enumerate() {
        let (_, dt, _) = infer_projection_field(&proj.expr, proj.alias.as_deref(), &combined)?;
        let arr = match dt {
            DataType::Int64 => Arc::new(arrow::array::Int64Array::from(
                output_agg_columns[agg_idx]
                    .iter()
                    .map(|v| *v as i64)
                    .collect::<Vec<_>>(),
            )) as arrow::array::ArrayRef,
            DataType::Float64 => Arc::new(arrow::array::Float64Array::from(
                output_agg_columns[agg_idx].clone(),
            )) as arrow::array::ArrayRef,
            _ => Arc::new(arrow::array::Int64Array::from(
                output_agg_columns[agg_idx]
                    .iter()
                    .map(|v| *v as i64)
                    .collect::<Vec<_>>(),
            )) as arrow::array::ArrayRef,
        };
        arrays.push(arr);
    }

    let out_batch = RecordBatch::try_new(out_schema, arrays)
        .map_err(|e| NanoError::Execution(e.to_string()))?;

    Ok(vec![out_batch])
}

fn compute_aggregate(func: &AggFunc, col: &arrow::array::ArrayRef, rows: &[usize]) -> Result<f64> {
    match func {
        AggFunc::Count => Ok(rows.len() as f64),
        AggFunc::Sum | AggFunc::Avg | AggFunc::Min | AggFunc::Max => {
            let mut values = Vec::new();
            for &row in rows {
                if let Some(v) = array_value_to_f64(col, row) {
                    values.push(v);
                }
            }
            if values.is_empty() {
                return match func {
                    AggFunc::Sum => Ok(0.0),
                    _ => Ok(f64::NAN), // min/max/avg of nothing = NaN
                };
            }
            match func {
                AggFunc::Sum => Ok(values.iter().sum()),
                AggFunc::Avg => Ok(values.iter().sum::<f64>() / values.len() as f64),
                AggFunc::Min => Ok(values.iter().cloned().fold(f64::INFINITY, f64::min)),
                AggFunc::Max => Ok(values.iter().cloned().fold(f64::NEG_INFINITY, f64::max)),
                _ => unreachable!(),
            }
        }
    }
}

fn array_value_to_string(arr: &arrow::array::ArrayRef, row: usize) -> String {
    use arrow::array::*;
    if arr.is_null(row) {
        return "NULL".to_string();
    }
    if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
        return a.value(row).to_string();
    }
    if let Some(a) = arr.as_any().downcast_ref::<Int32Array>() {
        return a.value(row).to_string();
    }
    if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
        return a.value(row).to_string();
    }
    if let Some(a) = arr.as_any().downcast_ref::<UInt64Array>() {
        return a.value(row).to_string();
    }
    if let Some(a) = arr.as_any().downcast_ref::<Float64Array>() {
        return a.value(row).to_string();
    }
    if let Some(a) = arr.as_any().downcast_ref::<BooleanArray>() {
        return a.value(row).to_string();
    }
    format!("?")
}

fn array_value_to_f64(arr: &arrow::array::ArrayRef, row: usize) -> Option<f64> {
    use arrow::array::*;
    if arr.is_null(row) {
        return None;
    }
    if let Some(a) = arr.as_any().downcast_ref::<Int32Array>() {
        return Some(a.value(row) as f64);
    }
    if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
        return Some(a.value(row) as f64);
    }
    if let Some(a) = arr.as_any().downcast_ref::<UInt64Array>() {
        return Some(a.value(row) as f64);
    }
    if let Some(a) = arr.as_any().downcast_ref::<Float64Array>() {
        return Some(a.value(row));
    }
    if let Some(a) = arr.as_any().downcast_ref::<Float32Array>() {
        return Some(a.value(row) as f64);
    }
    None
}

fn strings_to_array(values: &[String], dt: &DataType) -> arrow::array::ArrayRef {
    match dt {
        DataType::Utf8 => Arc::new(arrow::array::StringArray::from(
            values.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
        )),
        DataType::Int32 => Arc::new(arrow::array::Int32Array::from(
            values
                .iter()
                .map(|s| s.parse::<i32>().unwrap_or(0))
                .collect::<Vec<_>>(),
        )),
        DataType::Int64 => Arc::new(arrow::array::Int64Array::from(
            values
                .iter()
                .map(|s| s.parse::<i64>().unwrap_or(0))
                .collect::<Vec<_>>(),
        )),
        DataType::UInt64 => Arc::new(arrow::array::UInt64Array::from(
            values
                .iter()
                .map(|s| s.parse::<u64>().unwrap_or(0))
                .collect::<Vec<_>>(),
        )),
        _ => Arc::new(arrow::array::StringArray::from(
            values.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
        )),
    }
}

fn apply_order_and_limit(
    batches: &[RecordBatch],
    order_by: &[IROrdering],
    limit: Option<u64>,
) -> Result<Vec<RecordBatch>> {
    if batches.is_empty() {
        return Ok(vec![]);
    }

    // Concat all batches
    let schema = batches[0].schema();
    let combined = if batches.len() == 1 {
        batches[0].clone()
    } else {
        compute::concat_batches(&schema, batches)
            .map_err(|e| NanoError::Execution(e.to_string()))?
    };

    if combined.num_rows() == 0 {
        return Ok(vec![combined]);
    }

    let mut result = combined;

    // Apply ordering
    if !order_by.is_empty() {
        // Build sort columns
        let mut sort_columns = Vec::new();
        for ord in order_by {
            let col = match &ord.expr {
                IRExpr::PropAccess { variable, property } => {
                    let col_idx = result.schema().index_of(variable).ok();
                    if let Some(idx) = col_idx {
                        let struct_arr = result.column(idx).as_any().downcast_ref::<StructArray>();
                        struct_arr.and_then(|s| s.column_by_name(property).cloned())
                    } else {
                        // Try as a flat column (post-projection)
                        result
                            .schema()
                            .index_of(property)
                            .ok()
                            .map(|i| result.column(i).clone())
                    }
                }
                IRExpr::AliasRef(name) => result
                    .schema()
                    .index_of(name)
                    .ok()
                    .map(|i| result.column(i).clone()),
                _ => None,
            };

            if let Some(c) = col {
                sort_columns.push(arrow::compute::SortColumn {
                    values: c,
                    options: Some(arrow::compute::SortOptions {
                        descending: ord.descending,
                        nulls_first: false,
                    }),
                });
            }
        }

        if !sort_columns.is_empty() {
            let indices = arrow::compute::lexsort_to_indices(&sort_columns, None)
                .map_err(|e| NanoError::Execution(e.to_string()))?;

            let mut new_columns = Vec::new();
            for col in result.columns() {
                let taken = arrow::compute::take(col.as_ref(), &indices, None)
                    .map_err(|e| NanoError::Execution(e.to_string()))?;
                new_columns.push(taken);
            }
            result = RecordBatch::try_new(result.schema(), new_columns)
                .map_err(|e| NanoError::Execution(e.to_string()))?;
        }
    }

    // Apply limit
    if let Some(limit) = limit {
        let limit = limit as usize;
        if result.num_rows() > limit {
            result = result.slice(0, limit);
        }
    }

    Ok(vec![result])
}

/// A simple cross-join exec for combining multiple NodeScans
#[derive(Debug)]
struct CrossJoinExec {
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    output_schema: SchemaRef,
    properties: PlanProperties,
}

impl CrossJoinExec {
    fn new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        output_schema: SchemaRef,
    ) -> Self {
        let properties = PlanProperties::new(
            datafusion::physical_expr::EquivalenceProperties::new(output_schema.clone()),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
            datafusion::physical_plan::execution_plan::EmissionType::Incremental,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        );
        Self {
            left,
            right,
            output_schema,
            properties,
        }
    }
}

impl datafusion::physical_plan::DisplayAs for CrossJoinExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "CrossJoinExec")
    }
}

impl ExecutionPlan for CrossJoinExec {
    fn name(&self) -> &str {
        "CrossJoinExec"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.left, &self.right]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(CrossJoinExec::new(
            children[0].clone(),
            children[1].clone(),
            self.output_schema.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::context::TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        enum CrossJoinState {
            Init {
                left_stream: SendableRecordBatchStream,
                right_stream: SendableRecordBatchStream,
                schema: SchemaRef,
            },
            Running {
                left_stream: SendableRecordBatchStream,
                right_batches: Vec<RecordBatch>,
                current_left: Option<RecordBatch>,
                right_idx: usize,
                schema: SchemaRef,
            },
        }

        let left_stream = self.left.execute(partition, context.clone())?;
        let right_stream = self.right.execute(partition, context)?;

        let init = CrossJoinState::Init {
            left_stream,
            right_stream,
            schema: self.output_schema.clone(),
        };

        let stream = futures::stream::try_unfold(init, |state| async move {
            use futures::StreamExt;

            let mut state = state;
            loop {
                match state {
                    CrossJoinState::Init {
                        left_stream,
                        mut right_stream,
                        schema,
                    } => {
                        let mut right_batches = Vec::new();
                        while let Some(batch) = right_stream.next().await {
                            right_batches.push(batch?);
                        }

                        if right_batches.is_empty() {
                            return Ok(None);
                        }

                        state = CrossJoinState::Running {
                            left_stream,
                            right_batches,
                            current_left: None,
                            right_idx: 0,
                            schema,
                        };
                    }
                    CrossJoinState::Running {
                        mut left_stream,
                        right_batches,
                        mut current_left,
                        mut right_idx,
                        schema,
                    } => {
                        if current_left.is_none() {
                            let next_left = match left_stream.next().await {
                                Some(batch) => batch?,
                                None => return Ok(None),
                            };
                            current_left = Some(next_left);
                            right_idx = 0;
                        }

                        let left_batch = current_left.as_ref().expect("left batch set");

                        while right_idx < right_batches.len() {
                            let right_batch = &right_batches[right_idx];
                            right_idx += 1;
                            let cross = cross_join_batches(left_batch, right_batch, &schema)?;
                            if cross.num_rows() > 0 {
                                let next_state = CrossJoinState::Running {
                                    left_stream,
                                    right_batches,
                                    current_left,
                                    right_idx,
                                    schema,
                                };
                                return Ok(Some((cross, next_state)));
                            }
                        }

                        state = CrossJoinState::Running {
                            left_stream,
                            right_batches,
                            current_left: None,
                            right_idx: 0,
                            schema,
                        };
                    }
                }
            }
        });

        Ok(Box::pin(
            datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
                self.output_schema.clone(),
                stream,
            ),
        ))
    }
}

fn cross_join_batches(
    left: &RecordBatch,
    right: &RecordBatch,
    schema: &SchemaRef,
) -> DFResult<RecordBatch> {
    let left_rows = left.num_rows();
    let right_rows = right.num_rows();
    let total = left_rows * right_rows;

    if total == 0 {
        return Ok(RecordBatch::new_empty(schema.clone()));
    }

    let mut columns: Vec<arrow::array::ArrayRef> = Vec::new();

    // Replicate left columns
    for col in left.columns() {
        let mut indices = Vec::with_capacity(total);
        for i in 0..left_rows {
            for _ in 0..right_rows {
                indices.push(i as u64);
            }
        }
        let idx = arrow::array::UInt64Array::from(indices);
        let taken = arrow::compute::take(col.as_ref(), &idx, None)?;
        columns.push(taken);
    }

    // Replicate right columns
    for col in right.columns() {
        let mut indices = Vec::with_capacity(total);
        for _ in 0..left_rows {
            for j in 0..right_rows {
                indices.push(j as u64);
            }
        }
        let idx = arrow::array::UInt64Array::from(indices);
        let taken = arrow::compute::take(col.as_ref(), &idx, None)?;
        columns.push(taken);
    }

    RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
}

/// Anti-join exec: returns rows from left where no matching rows exist in right
#[derive(Debug)]
struct AntiJoinExec {
    outer: Arc<dyn ExecutionPlan>,
    inner: Arc<dyn ExecutionPlan>,
    join_var: String,
    inner_pipeline: Vec<IROp>,
    params: ParamMap,
    storage: Arc<GraphStorage>,
    output_schema: SchemaRef,
    properties: PlanProperties,
}

impl AntiJoinExec {
    fn new(
        outer: Arc<dyn ExecutionPlan>,
        inner: Arc<dyn ExecutionPlan>,
        join_var: String,
        inner_pipeline: Vec<IROp>,
        params: ParamMap,
        storage: Arc<GraphStorage>,
    ) -> Self {
        let output_schema = outer.schema();
        let properties = PlanProperties::new(
            datafusion::physical_expr::EquivalenceProperties::new(output_schema.clone()),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
            datafusion::physical_plan::execution_plan::EmissionType::Incremental,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        );
        Self {
            outer,
            inner,
            join_var,
            inner_pipeline,
            params,
            storage,
            output_schema,
            properties,
        }
    }
}

impl DisplayAs for AntiJoinExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "AntiJoinExec on ${}", self.join_var)
    }
}

impl ExecutionPlan for AntiJoinExec {
    fn name(&self) -> &str {
        "AntiJoinExec"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.outer, &self.inner]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(AntiJoinExec::new(
            children[0].clone(),
            children[1].clone(),
            self.join_var.clone(),
            self.inner_pipeline.clone(),
            self.params.clone(),
            self.storage.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::context::TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        enum AntiJoinState {
            Init {
                outer_stream: SendableRecordBatchStream,
                inner_stream: SendableRecordBatchStream,
                join_var: String,
                inner_pipeline: Vec<IROp>,
                params: ParamMap,
            },
            Running {
                outer_stream: SendableRecordBatchStream,
                inner_ids: AHashSet<u64>,
                join_var: String,
            },
        }

        let outer_stream = self.outer.execute(partition, context.clone())?;
        let inner_stream = self.inner.execute(partition, context)?;

        let init = AntiJoinState::Init {
            outer_stream,
            inner_stream,
            join_var: self.join_var.clone(),
            inner_pipeline: self.inner_pipeline.clone(),
            params: self.params.clone(),
        };

        let stream = futures::stream::try_unfold(init, |state| async move {
            use futures::StreamExt;

            let mut state = state;
            loop {
                match state {
                    AntiJoinState::Init {
                        outer_stream,
                        mut inner_stream,
                        join_var,
                        inner_pipeline,
                        params,
                    } => {
                        let mut inner_ids: AHashSet<u64> = AHashSet::new();

                        while let Some(batch) = inner_stream.next().await {
                            let batch = batch?;
                            let filtered_batches =
                                apply_ir_filters(&inner_pipeline, &[batch], &params).map_err(
                                    |e| {
                                        datafusion::error::DataFusionError::Execution(e.to_string())
                                    },
                                )?;

                            for filtered in filtered_batches {
                                if let Ok(col_idx) = filtered.schema().index_of(&join_var) {
                                    let col = filtered.column(col_idx);
                                    if let Some(struct_arr) =
                                        col.as_any().downcast_ref::<StructArray>()
                                    {
                                        if let Some(id_col) = struct_arr.column_by_name("id") {
                                            if let Some(id_arr) = id_col
                                                .as_any()
                                                .downcast_ref::<arrow::array::UInt64Array>(
                                            ) {
                                                for i in 0..id_arr.len() {
                                                    inner_ids.insert(id_arr.value(i));
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        state = AntiJoinState::Running {
                            outer_stream,
                            inner_ids,
                            join_var,
                        };
                    }
                    AntiJoinState::Running {
                        mut outer_stream,
                        inner_ids,
                        join_var,
                    } => {
                        while let Some(batch) = outer_stream.next().await {
                            let batch = batch?;
                            let col_idx = batch.schema().index_of(&join_var)?;
                            let col = batch.column(col_idx);
                            let struct_arr =
                                col.as_any().downcast_ref::<StructArray>().ok_or_else(|| {
                                    datafusion::error::DataFusionError::Execution(format!(
                                        "column {} is not a struct",
                                        join_var
                                    ))
                                })?;
                            let id_col = struct_arr.column_by_name("id").ok_or_else(|| {
                                datafusion::error::DataFusionError::Execution(format!(
                                    "struct {} has no id field",
                                    join_var
                                ))
                            })?;
                            let id_arr = id_col
                                .as_any()
                                .downcast_ref::<arrow::array::UInt64Array>()
                                .ok_or_else(|| {
                                    datafusion::error::DataFusionError::Execution(format!(
                                        "struct {} id field is not UInt64",
                                        join_var
                                    ))
                                })?;

                            let mask = arrow::array::BooleanArray::from(
                                (0..id_arr.len())
                                    .map(|i| !inner_ids.contains(&id_arr.value(i)))
                                    .collect::<Vec<_>>(),
                            );
                            let filtered = arrow::compute::filter_record_batch(&batch, &mask)?;
                            if filtered.num_rows() > 0 {
                                let next_state = AntiJoinState::Running {
                                    outer_stream,
                                    inner_ids,
                                    join_var,
                                };
                                return Ok(Some((filtered, next_state)));
                            }
                        }

                        return Ok(None);
                    }
                }
            }
        });

        Ok(Box::pin(
            datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
                self.output_schema.clone(),
                stream,
            ),
        ))
    }
}
