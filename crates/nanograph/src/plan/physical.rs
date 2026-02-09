use std::any::Any;
use std::fmt;
use std::sync::Arc;

use ahash::AHashMap;
use arrow::array::{Array, ArrayRef, RecordBatch, StructArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::Result;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};

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
    ) -> Result<Arc<dyn ExecutionPlan>> {
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
    ) -> Result<SendableRecordBatchStream> {
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
    fn expand_batch(&self, input: &RecordBatch) -> Result<RecordBatch> {
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
fn take_rows(array: &ArrayRef, indices: &[usize]) -> Result<ArrayRef> {
    let idx_array = UInt64Array::from(indices.iter().map(|&i| i as u64).collect::<Vec<_>>());
    let taken = arrow::compute::take(array.as_ref(), &idx_array, None)?;
    Ok(taken)
}
