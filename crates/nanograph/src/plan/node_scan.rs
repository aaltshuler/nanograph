use std::any::Any;
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray, StructArray,
};
use arrow::datatypes::{DataType, Field, SchemaRef};
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use futures::StreamExt;
use lance::Dataset;
use tracing::debug;

use crate::query::ast::{CompOp, Literal};
use crate::store::graph::GraphStorage;

#[derive(Debug, Clone)]
pub(crate) struct NodeScanPredicate {
    pub(crate) property: String,
    pub(crate) op: CompOp,
    pub(crate) literal: Literal,
    pub(crate) index_eligible: bool,
}

/// Physical execution plan that scans all nodes of a type, outputting a Struct column.
#[derive(Debug)]
pub(crate) struct NodeScanExec {
    type_name: String,
    variable_name: String,
    output_schema: SchemaRef,
    pushdown_filters: Vec<NodeScanPredicate>,
    limit: Option<usize>,
    storage: Arc<GraphStorage>,
    properties: PlanProperties,
}

impl NodeScanExec {
    pub(crate) fn new(
        type_name: String,
        variable_name: String,
        output_schema: SchemaRef,
        pushdown_filters: Vec<NodeScanPredicate>,
        limit: Option<usize>,
        storage: Arc<GraphStorage>,
    ) -> Self {
        let properties = PlanProperties::new(
            datafusion::physical_expr::EquivalenceProperties::new(output_schema.clone()),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
            datafusion::physical_plan::execution_plan::EmissionType::Incremental,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        );

        Self {
            type_name,
            variable_name,
            output_schema,
            pushdown_filters,
            limit,
            storage,
            properties,
        }
    }

    fn literal_to_array(literal: &Literal, num_rows: usize) -> ArrayRef {
        match literal {
            Literal::String(s) => Arc::new(StringArray::from(vec![s.as_str(); num_rows])),
            Literal::Integer(v) => Arc::new(Int64Array::from(vec![*v; num_rows])),
            Literal::Float(v) => Arc::new(Float64Array::from(vec![*v; num_rows])),
            Literal::Bool(v) => Arc::new(BooleanArray::from(vec![*v; num_rows])),
        }
    }

    fn compare_arrays(left: &ArrayRef, right: &ArrayRef, op: CompOp) -> Result<BooleanArray> {
        use arrow::compute::kernels::cmp;

        match op {
            CompOp::Eq => cmp::eq(left, right),
            CompOp::Ne => cmp::neq(left, right),
            CompOp::Gt => cmp::gt(left, right),
            CompOp::Lt => cmp::lt(left, right),
            CompOp::Ge => cmp::gt_eq(left, right),
            CompOp::Le => cmp::lt_eq(left, right),
        }
        .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))
    }

    fn apply_pushdown_filters(&self, input: &RecordBatch) -> Result<RecordBatch> {
        if self.pushdown_filters.is_empty() {
            return Ok(input.clone());
        }

        let mut current = input.clone();
        for predicate in &self.pushdown_filters {
            let left = current
                .column_by_name(&predicate.property)
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "column {} not found during node scan pushdown",
                        predicate.property
                    ))
                })?
                .clone();

            let right = Self::literal_to_array(&predicate.literal, current.num_rows());
            let right = if left.data_type() != right.data_type() {
                arrow::compute::cast(&right, left.data_type())
                    .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?
            } else {
                right
            };

            let mask = Self::compare_arrays(&left, &right, predicate.op)?;
            current = arrow::compute::filter_record_batch(&current, &mask)
                .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;

            if current.num_rows() == 0 {
                break;
            }
        }

        Ok(current)
    }

    fn output_struct_fields(output_schema: &SchemaRef) -> Result<Vec<Field>> {
        let struct_field = output_schema.field(0);
        match struct_field.data_type() {
            DataType::Struct(fields) => Ok(fields
                .iter()
                .map(|f| f.as_ref().clone())
                .collect::<Vec<Field>>()),
            other => Err(DataFusionError::Execution(format!(
                "NodeScanExec expected struct output field, found {other:?}"
            ))),
        }
    }

    fn wrap_struct_batch_for_schema(
        output_schema: &SchemaRef,
        batch: &RecordBatch,
    ) -> Result<RecordBatch> {
        let struct_fields = Self::output_struct_fields(output_schema)?;
        let mut struct_columns = Vec::with_capacity(struct_fields.len());
        for field in &struct_fields {
            let col = batch.column_by_name(field.name()).ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "column {} not found while materializing node scan output",
                    field.name()
                ))
            })?;
            struct_columns.push(col.clone());
        }

        let struct_array = StructArray::new(struct_fields.into(), struct_columns, None);
        RecordBatch::try_new(
            output_schema.clone(),
            vec![Arc::new(struct_array) as ArrayRef],
        )
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
    }

    fn wrap_struct_batch(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        Self::wrap_struct_batch_for_schema(&self.output_schema, batch)
    }

    fn projected_column_names(&self) -> Result<Vec<String>> {
        Ok(Self::output_struct_fields(&self.output_schema)?
            .into_iter()
            .map(|f| f.name().clone())
            .collect())
    }

    fn literal_to_lance_sql(literal: &Literal) -> Option<String> {
        match literal {
            Literal::String(s) => Some(format!("'{}'", s.replace('\'', "''"))),
            Literal::Integer(v) => Some(v.to_string()),
            Literal::Float(v) => {
                if v.is_finite() {
                    Some(v.to_string())
                } else {
                    None
                }
            }
            Literal::Bool(v) => Some(if *v { "true" } else { "false" }.to_string()),
        }
    }

    fn lance_filter_sql(&self) -> std::result::Result<Option<String>, String> {
        if self.pushdown_filters.is_empty() {
            return Ok(None);
        }

        let mut clauses = Vec::with_capacity(self.pushdown_filters.len());
        for pred in &self.pushdown_filters {
            let op = match pred.op {
                CompOp::Eq => "=",
                CompOp::Ne => "!=",
                CompOp::Gt => ">",
                CompOp::Lt => "<",
                CompOp::Ge => ">=",
                CompOp::Le => "<=",
            };
            let lit = Self::literal_to_lance_sql(&pred.literal).ok_or_else(|| {
                format!(
                    "unsupported literal in Lance filter pushdown for property {}",
                    pred.property
                )
            })?;
            clauses.push(format!("{} {} {}", pred.property, op, lit));
        }

        Ok(Some(clauses.join(" AND ")))
    }

    fn maybe_lance_dataset_path(&self) -> Option<PathBuf> {
        self.storage
            .node_dataset_path(&self.type_name)
            .map(|p| p.to_path_buf())
            .filter(|p| p.exists())
    }

    fn has_index_eligible_pushdown(&self) -> bool {
        self.pushdown_filters.iter().any(|p| p.index_eligible)
    }
}

impl DisplayAs for NodeScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "NodeScanExec: ${}: {} (filters={}, index_eligible={})",
            self.variable_name,
            self.type_name,
            self.pushdown_filters.len(),
            self.has_index_eligible_pushdown()
        )
    }
}

impl ExecutionPlan for NodeScanExec {
    fn name(&self) -> &str {
        "NodeScanExec"
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
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if let Some(dataset_path) = self.maybe_lance_dataset_path() {
            let output_schema = self.output_schema.clone();
            let projected_columns = self.projected_column_names()?;
            let filter_sql = match self.lance_filter_sql() {
                Ok(sql) => sql,
                Err(err) => {
                    debug!(
                        node_type = %self.type_name,
                        reason = %err,
                        "falling back to in-memory path: cannot convert predicate to Lance SQL"
                    );
                    return self.execute_in_memory_scan();
                }
            };
            let filter_sql_for_debug = filter_sql.clone();
            let limit = self.limit.and_then(|v| i64::try_from(v).ok());
            let stream = futures::stream::once(async move {
                let uri = dataset_path.to_string_lossy().to_string();
                let dataset = Dataset::open(&uri).await.map_err(|e| {
                    DataFusionError::Execution(format!("lance dataset open error: {}", e))
                })?;
                let mut scanner = dataset.scan();
                scanner.project(&projected_columns).map_err(|e| {
                    DataFusionError::Execution(format!("lance projection pushdown error: {}", e))
                })?;
                if let Some(expr) = filter_sql.as_deref() {
                    scanner.filter(expr).map_err(|e| {
                        DataFusionError::Execution(format!("lance predicate pushdown error: {}", e))
                    })?;
                }
                if let Some(lim) = limit {
                    scanner.limit(Some(lim), None).map_err(|e| {
                        DataFusionError::Execution(format!("lance limit pushdown error: {}", e))
                    })?;
                }

                let mut scan_stream = scanner.try_into_stream().await.map_err(|e| {
                    DataFusionError::Execution(format!("lance scanner stream error: {}", e))
                })?;
                let mut batches = Vec::new();
                while let Some(batch) = scan_stream.next().await {
                    batches.push(batch.map_err(|e| {
                        DataFusionError::Execution(format!("lance stream batch error: {}", e))
                    })?);
                }

                if batches.is_empty() {
                    return Ok(RecordBatch::new_empty(output_schema.clone()));
                }

                let merged = if batches.len() == 1 {
                    batches.remove(0)
                } else {
                    let projected_schema = batches[0].schema();
                    arrow::compute::concat_batches(&projected_schema, &batches).map_err(|e| {
                        DataFusionError::Execution(format!(
                            "concat Lance scan batches error: {}",
                            e
                        ))
                    })?
                };

                NodeScanExec::wrap_struct_batch_for_schema(&output_schema, &merged)
            });

            debug!(
                node_type = %self.type_name,
                filter_sql = ?filter_sql_for_debug,
                limit = ?self.limit,
                index_eligible = self.has_index_eligible_pushdown(),
                "using Lance-native node scan pushdown path"
            );

            return Ok(Box::pin(
                datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
                    self.output_schema.clone(),
                    stream,
                ),
            ));
        }

        self.execute_in_memory_scan()
    }
}

impl NodeScanExec {
    fn execute_in_memory_scan(&self) -> Result<SendableRecordBatchStream> {
        debug!(
            node_type = %self.type_name,
            "using in-memory node scan fallback path"
        );
        let mut output_batches = Vec::new();
        let mut remaining = self.limit.unwrap_or(usize::MAX);

        if let Some(segment) = self.storage.node_segments.get(&self.type_name) {
            for batch in &segment.batches {
                if remaining == 0 {
                    break;
                }

                let filtered = self.apply_pushdown_filters(batch)?;
                if filtered.num_rows() == 0 {
                    continue;
                }

                let rows_to_emit = filtered.num_rows().min(remaining);
                let filtered = if rows_to_emit < filtered.num_rows() {
                    filtered.slice(0, rows_to_emit)
                } else {
                    filtered
                };

                remaining = remaining.saturating_sub(rows_to_emit);
                output_batches.push(self.wrap_struct_batch(&filtered)?);
            }
        }

        if output_batches.is_empty() {
            output_batches.push(RecordBatch::new_empty(self.output_schema.clone()));
        }

        Ok(Box::pin(MemoryStream::try_new(
            output_batches,
            self.output_schema.clone(),
            None,
        )?))
    }
}
