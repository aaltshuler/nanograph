use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use ahash::AHashMap;
use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array,
    Int32Array, Int64Array, RecordBatch, StringArray, StructArray, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::Result as DFResult;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};

use crate::error::{NanoError, Result};
use crate::ir::{IRAssignment, IRExpr, IRMutationPredicate, MutationIR, MutationOpIR, ParamMap};
use crate::query::ast::{CompOp, Literal};
use crate::store::database::{Database, DeleteOp, DeletePredicate, LoadMode};
use crate::store::graph::GraphStorage;
use crate::types::Direction;

/// Physical execution plan that expands from source nodes along an edge type,
/// producing new destination struct columns.
#[derive(Debug)]
pub struct ExpandExec {
    input: Arc<dyn ExecutionPlan>,
    src_var: String,
    dst_var: String,
    edge_type: String,
    direction: Direction,
    dst_type: String,
    output_schema: SchemaRef,
    storage: Arc<GraphStorage>,
    properties: PlanProperties,
}

impl ExpandExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        src_var: String,
        dst_var: String,
        edge_type: String,
        direction: Direction,
        dst_type: String,
        storage: Arc<GraphStorage>,
    ) -> Self {
        let input_schema = input.schema();

        // Build output schema: input fields + new struct column for dst
        let dst_node_type = &storage.catalog.node_types[&dst_type];
        let dst_struct_fields: Vec<Field> = dst_node_type
            .arrow_schema
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        let dst_field = Field::new(&dst_var, DataType::Struct(dst_struct_fields.into()), false);

        let mut output_fields: Vec<Field> = input_schema
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        output_fields.push(dst_field);
        let output_schema = Arc::new(Schema::new(output_fields));

        let properties = PlanProperties::new(
            datafusion::physical_expr::EquivalenceProperties::new(output_schema.clone()),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
            datafusion::physical_plan::execution_plan::EmissionType::Incremental,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        );

        Self {
            input,
            src_var,
            dst_var,
            edge_type,
            direction,
            dst_type,
            output_schema,
            storage,
            properties,
        }
    }
}

impl DisplayAs for ExpandExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "ExpandExec: ${} --[{}]--> ${}",
            self.src_var, self.edge_type, self.dst_var
        )
    }
}

impl ExecutionPlan for ExpandExec {
    fn name(&self) -> &str {
        "ExpandExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(ExpandExec::new(
            children[0].clone(),
            self.src_var.clone(),
            self.dst_var.clone(),
            self.edge_type.clone(),
            self.direction,
            self.dst_type.clone(),
            self.storage.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let input = self.input.clone();
        let schema = self.output_schema.clone();
        let edge_type = self.edge_type.clone();
        let direction = self.direction;
        let src_var = self.src_var.clone();
        let dst_var = self.dst_var.clone();
        let dst_type = self.dst_type.clone();
        let storage = self.storage.clone();

        let stream = futures::stream::once(async move {
            use datafusion::physical_plan::common::collect;
            let batches = collect(input.execute(partition, context)?).await?;

            // Create an ExpandExec with the real input plan for correct output_schema
            let expand = ExpandExec {
                input: Arc::new(datafusion::physical_plan::empty::EmptyExec::new(Arc::new(
                    Schema::new(Vec::<Field>::new()),
                ))),
                src_var,
                dst_var,
                edge_type,
                direction,
                dst_type,
                output_schema: schema.clone(),
                storage,
                properties: PlanProperties::new(
                    datafusion::physical_expr::EquivalenceProperties::new(schema.clone()),
                    datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
                    datafusion::physical_plan::execution_plan::EmissionType::Incremental,
                    datafusion::physical_plan::execution_plan::Boundedness::Bounded,
                ),
            };

            let mut result_columns: Vec<Vec<ArrayRef>> = Vec::new();
            let mut total_rows = 0usize;

            for batch in &batches {
                let expanded = expand.expand_batch(batch)?;
                if expanded.num_rows() > 0 {
                    if result_columns.is_empty() {
                        result_columns.resize(expanded.num_columns(), Vec::new());
                    }
                    total_rows += expanded.num_rows();
                    for (i, col) in expanded.columns().iter().enumerate() {
                        result_columns[i].push(col.clone());
                    }
                }
            }

            if total_rows == 0 {
                return Ok(RecordBatch::new_empty(schema));
            }

            let mut final_cols = Vec::new();
            for col_arrays in &result_columns {
                let refs: Vec<&dyn arrow::array::Array> =
                    col_arrays.iter().map(|a| a.as_ref()).collect();
                let concatenated = arrow::compute::concat(&refs)?;
                final_cols.push(concatenated);
            }

            RecordBatch::try_new(schema, final_cols)
                .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
        });

        Ok(Box::pin(
            datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
                self.output_schema.clone(),
                stream,
            ),
        ))
    }
}

impl ExpandExec {
    fn expand_batch(&self, input: &RecordBatch) -> DFResult<RecordBatch> {
        let edge_seg = self
            .storage
            .edge_segments
            .get(&self.edge_type)
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(format!(
                    "edge type {} not found",
                    self.edge_type
                ))
            })?;

        let csr = match self.direction {
            Direction::Out => edge_seg.csr.as_ref(),
            Direction::In => edge_seg.csc.as_ref(),
        }
        .ok_or_else(|| {
            datafusion::error::DataFusionError::Execution("CSR not built".to_string())
        })?;

        // Find the src struct column
        let src_col_idx = input
            .schema()
            .index_of(&self.src_var)
            .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;
        let src_struct = input
            .column(src_col_idx)
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(
                    "source column is not a struct".to_string(),
                )
            })?;

        // Get the id field from the struct
        let id_col = src_struct.column_by_name("id").ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(
                "no id field in source struct".to_string(),
            )
        })?;
        let id_array = id_col
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution("id field is not UInt64".to_string())
            })?;

        // Get destination node data
        let dst_all_nodes = self
            .storage
            .get_all_nodes(&self.dst_type)
            .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;
        let dst_batch = match &dst_all_nodes {
            Some(b) => b,
            None => {
                return Ok(RecordBatch::new_empty(self.output_schema.clone()));
            }
        };

        // Build id -> row mapping from the concatenated dst batch
        // (can't use segment.id_to_row because it points at original multi-batch indices)
        let dst_id_col = dst_batch
            .column(0) // id is always the first column
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(
                    "dst id column is not UInt64".to_string(),
                )
            })?;
        let mut dst_id_to_row: AHashMap<u64, usize> = AHashMap::new();
        for row in 0..dst_id_col.len() {
            dst_id_to_row.insert(dst_id_col.value(row), row);
        }

        // For each input row, look up neighbors and expand
        let mut output_row_indices: Vec<(usize, u64)> = Vec::new(); // (input_row, dst_node_id)

        for row in 0..input.num_rows() {
            let src_id = id_array.value(row);
            let neighbors = csr.neighbors(src_id);
            for &dst_id in neighbors {
                output_row_indices.push((row, dst_id));
            }
        }

        if output_row_indices.is_empty() {
            return Ok(RecordBatch::new_empty(self.output_schema.clone()));
        }

        // Resolve destination rows and fail fast if edge references a missing destination.
        let mut valid_indices: Vec<(usize, usize)> = Vec::with_capacity(output_row_indices.len());
        for (src_idx, dst_id) in &output_row_indices {
            let dst_row = dst_id_to_row.get(dst_id).copied().ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(format!(
                    "edge {} references missing destination node id {}",
                    self.edge_type, dst_id
                ))
            })?;
            valid_indices.push((*src_idx, dst_row));
        }

        if valid_indices.is_empty() {
            return Ok(RecordBatch::new_empty(self.output_schema.clone()));
        }

        // Build output columns
        let mut output_columns: Vec<ArrayRef> = Vec::new();

        // Replicate input columns based on expansion
        let input_row_indices: Vec<usize> = valid_indices.iter().map(|(r, _)| *r).collect();
        for col_idx in 0..input.num_columns() {
            let col = input.column(col_idx);
            let taken = take_rows(col, &input_row_indices)?;
            output_columns.push(taken);
        }

        // Build destination struct column
        let dst_schema = dst_batch.schema();
        let mut dst_field_arrays: Vec<ArrayRef> = Vec::with_capacity(dst_schema.fields().len());

        let dst_row_indices: Vec<usize> = valid_indices.iter().map(|(_, dr)| *dr).collect();
        for field_idx in 0..dst_schema.fields().len() {
            let dst_col = dst_batch.column(field_idx);
            let taken = take_rows(dst_col, &dst_row_indices)?;
            dst_field_arrays.push(taken);
        }

        let dst_struct_fields: Vec<Field> = dst_schema
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        let dst_struct_array = StructArray::new(dst_struct_fields.into(), dst_field_arrays, None);
        output_columns.push(Arc::new(dst_struct_array));

        RecordBatch::try_new(self.output_schema.clone(), output_columns)
            .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
    }
}

/// Take specific rows from an array by index.
fn take_rows(array: &ArrayRef, indices: &[usize]) -> DFResult<ArrayRef> {
    let idx_array = UInt64Array::from(indices.iter().map(|&i| i as u64).collect::<Vec<_>>());
    let taken = arrow::compute::take(array.as_ref(), &idx_array, None)?;
    Ok(taken)
}

#[derive(Debug, Clone, Copy, Default)]
pub struct MutationExecResult {
    pub affected_nodes: usize,
    pub affected_edges: usize,
}

pub async fn execute_mutation(
    ir: &MutationIR,
    db: &mut Database,
    params: &ParamMap,
) -> Result<MutationExecResult> {
    match &ir.op {
        MutationOpIR::Insert {
            type_name,
            assignments,
        } => execute_insert_mutation(db, type_name, assignments, params).await,
        MutationOpIR::Update {
            type_name,
            assignments,
            predicate,
        } => execute_update_mutation(db, type_name, assignments, predicate, params).await,
        MutationOpIR::Delete {
            type_name,
            predicate,
        } => execute_delete_mutation(db, type_name, predicate, params).await,
    }
}

async fn execute_insert_mutation(
    db: &mut Database,
    type_name: &str,
    assignments: &[IRAssignment],
    params: &ParamMap,
) -> Result<MutationExecResult> {
    let mut data = serde_json::Map::new();
    for assignment in assignments {
        let lit = resolve_mutation_literal(&assignment.value, params)?;
        data.insert(assignment.property.clone(), literal_to_json(&lit)?);
    }
    let line = serde_json::json!({
        "type": type_name,
        "data": data
    })
    .to_string();
    db.load_with_mode(&line, LoadMode::Append).await?;
    Ok(MutationExecResult {
        affected_nodes: 1,
        affected_edges: 0,
    })
}

async fn execute_update_mutation(
    db: &mut Database,
    type_name: &str,
    assignments: &[IRAssignment],
    predicate: &IRMutationPredicate,
    params: &ParamMap,
) -> Result<MutationExecResult> {
    let key_prop = find_key_property(db, type_name).ok_or_else(|| {
        NanoError::Storage(format!(
            "update mutation requires @key on node type `{}` for identity-safe updates",
            type_name
        ))
    })?;
    if assignments.iter().any(|a| a.property == key_prop) {
        return Err(NanoError::Storage(format!(
            "update mutation cannot assign @key property `{}`",
            key_prop
        )));
    }

    let target_batch = match db.storage.get_all_nodes(type_name)? {
        Some(batch) => batch,
        None => return Ok(MutationExecResult::default()),
    };
    let delete_pred = build_delete_predicate(predicate, params)?;
    let match_mask =
        crate::store::database::build_delete_mask_for_mutation(&target_batch, &delete_pred)?;

    let matched_rows: Vec<usize> = (0..target_batch.num_rows())
        .filter(|&row| !match_mask.is_null(row) && match_mask.value(row))
        .collect();
    if matched_rows.is_empty() {
        return Ok(MutationExecResult::default());
    }

    let mut assignment_values = HashMap::new();
    for assignment in assignments {
        let lit = resolve_mutation_literal(&assignment.value, params)?;
        assignment_values.insert(assignment.property.clone(), literal_to_json(&lit)?);
    }

    let schema = target_batch.schema();
    let prop_columns: Vec<(usize, String)> = schema
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(idx, field)| {
            if idx == 0 {
                None
            } else {
                Some((idx, field.name().clone()))
            }
        })
        .collect();

    let mut payload_lines = Vec::with_capacity(matched_rows.len());
    for row in matched_rows.iter().copied() {
        let mut data_obj = serde_json::Map::new();
        for (idx, prop_name) in &prop_columns {
            let val = array_value_to_json(target_batch.column(*idx), row);
            data_obj.insert(prop_name.clone(), val);
        }
        for (prop, value) in &assignment_values {
            data_obj.insert(prop.clone(), value.clone());
        }

        payload_lines.push(
            serde_json::json!({
                "type": type_name,
                "data": data_obj
            })
            .to_string(),
        );
    }

    let payload = payload_lines.join("\n");
    db.load_with_mode(&payload, LoadMode::Merge).await?;
    Ok(MutationExecResult {
        affected_nodes: matched_rows.len(),
        affected_edges: 0,
    })
}

async fn execute_delete_mutation(
    db: &mut Database,
    type_name: &str,
    predicate: &IRMutationPredicate,
    params: &ParamMap,
) -> Result<MutationExecResult> {
    let delete_pred = build_delete_predicate(predicate, params)?;
    let result = db.delete_nodes(type_name, &delete_pred).await?;
    Ok(MutationExecResult {
        affected_nodes: result.deleted_nodes,
        affected_edges: result.deleted_edges,
    })
}

fn build_delete_predicate(
    predicate: &IRMutationPredicate,
    params: &ParamMap,
) -> Result<DeletePredicate> {
    let lit = resolve_mutation_literal(&predicate.value, params)?;
    Ok(DeletePredicate {
        property: predicate.property.clone(),
        op: comp_op_to_delete_op(predicate.op)?,
        value: literal_to_predicate_string(&lit)?,
    })
}

fn resolve_mutation_literal(expr: &IRExpr, params: &ParamMap) -> Result<Literal> {
    match expr {
        IRExpr::Literal(l) => Ok(l.clone()),
        IRExpr::Param(name) => params.get(name).cloned().ok_or_else(|| {
            NanoError::Execution(format!("missing required mutation parameter `${}`", name))
        }),
        other => Err(NanoError::Execution(format!(
            "mutation expression must be literal or parameter, got {:?}",
            other
        ))),
    }
}

fn comp_op_to_delete_op(op: CompOp) -> Result<DeleteOp> {
    match op {
        CompOp::Eq => Ok(DeleteOp::Eq),
        CompOp::Ne => Ok(DeleteOp::Ne),
        CompOp::Gt => Ok(DeleteOp::Gt),
        CompOp::Lt => Ok(DeleteOp::Lt),
        CompOp::Ge => Ok(DeleteOp::Ge),
        CompOp::Le => Ok(DeleteOp::Le),
    }
}

fn literal_to_json(lit: &Literal) -> Result<serde_json::Value> {
    match lit {
        Literal::String(s) => Ok(serde_json::Value::String(s.clone())),
        Literal::Integer(i) => Ok(serde_json::Value::Number((*i).into())),
        Literal::Float(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .ok_or_else(|| NanoError::Execution(format!("invalid float literal {}", f))),
        Literal::Bool(b) => Ok(serde_json::Value::Bool(*b)),
    }
}

fn literal_to_predicate_string(lit: &Literal) -> Result<String> {
    match lit {
        Literal::String(s) => Ok(s.clone()),
        Literal::Integer(i) => Ok(i.to_string()),
        Literal::Float(f) => {
            if !f.is_finite() {
                return Err(NanoError::Execution(format!("invalid float literal {}", f)));
            }
            Ok(f.to_string())
        }
        Literal::Bool(b) => Ok(b.to_string()),
    }
}

fn find_key_property(db: &Database, type_name: &str) -> Option<String> {
    db.schema_ir
        .node_types()
        .find(|n| n.name == type_name)
        .and_then(|n| n.properties.iter().find(|p| p.key).map(|p| p.name.clone()))
}

fn array_value_to_json(array: &ArrayRef, row: usize) -> serde_json::Value {
    if array.is_null(row) {
        return serde_json::Value::Null;
    }

    match array.data_type() {
        DataType::Utf8 => array
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|a| serde_json::Value::String(a.value(row).to_string()))
            .unwrap_or(serde_json::Value::Null),
        DataType::Boolean => array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .map(|a| serde_json::Value::Bool(a.value(row)))
            .unwrap_or(serde_json::Value::Null),
        DataType::Int32 => array
            .as_any()
            .downcast_ref::<Int32Array>()
            .map(|a| serde_json::Value::Number((a.value(row) as i64).into()))
            .unwrap_or(serde_json::Value::Null),
        DataType::Int64 => array
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|a| serde_json::Value::Number(a.value(row).into()))
            .unwrap_or(serde_json::Value::Null),
        DataType::UInt32 => array
            .as_any()
            .downcast_ref::<UInt32Array>()
            .map(|a| serde_json::Value::Number((a.value(row) as u64).into()))
            .unwrap_or(serde_json::Value::Null),
        DataType::UInt64 => array
            .as_any()
            .downcast_ref::<UInt64Array>()
            .map(|a| serde_json::Value::Number(a.value(row).into()))
            .unwrap_or(serde_json::Value::Null),
        DataType::Float32 => array
            .as_any()
            .downcast_ref::<Float32Array>()
            .and_then(|a| {
                serde_json::Number::from_f64(a.value(row) as f64).map(serde_json::Value::Number)
            })
            .unwrap_or(serde_json::Value::Null),
        DataType::Float64 => array
            .as_any()
            .downcast_ref::<Float64Array>()
            .and_then(|a| serde_json::Number::from_f64(a.value(row)).map(serde_json::Value::Number))
            .unwrap_or(serde_json::Value::Null),
        DataType::Date32 => array
            .as_any()
            .downcast_ref::<Date32Array>()
            .map(|a| serde_json::Value::Number((a.value(row) as i64).into()))
            .unwrap_or(serde_json::Value::Null),
        DataType::Date64 => array
            .as_any()
            .downcast_ref::<Date64Array>()
            .map(|a| serde_json::Value::Number(a.value(row).into()))
            .unwrap_or(serde_json::Value::Null),
        _ => serde_json::Value::Null,
    }
}
