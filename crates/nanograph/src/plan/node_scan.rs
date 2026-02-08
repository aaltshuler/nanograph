use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch, StructArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::common::Result;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use datafusion::prelude::Expr;

use crate::store::graph::GraphStorage;

/// TableProvider for a single node type. Registers with DataFusion as a table.
#[derive(Debug)]
pub struct NodeTypeTable {
    pub type_name: String,
    pub variable_name: String,
    pub struct_schema: SchemaRef,
    pub storage: Arc<GraphStorage>,
}

impl NodeTypeTable {
    pub fn new(
        type_name: String,
        variable_name: String,
        storage: Arc<GraphStorage>,
    ) -> Self {
        let node_type = &storage.catalog.node_types[&type_name];
        let struct_fields: Vec<Field> = node_type
            .arrow_schema
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        let struct_field = Field::new(
            &variable_name,
            DataType::Struct(struct_fields.into()),
            false,
        );
        let struct_schema = Arc::new(Schema::new(vec![struct_field]));

        Self {
            type_name,
            variable_name,
            struct_schema,
            storage,
        }
    }
}

#[async_trait::async_trait]
impl TableProvider for NodeTypeTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.struct_schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(NodeScanExec::new(
            self.type_name.clone(),
            self.variable_name.clone(),
            self.struct_schema.clone(),
            self.storage.clone(),
        )))
    }
}

/// Physical execution plan that scans all nodes of a type, outputting a Struct column.
#[derive(Debug)]
pub struct NodeScanExec {
    type_name: String,
    variable_name: String,
    output_schema: SchemaRef,
    storage: Arc<GraphStorage>,
    properties: PlanProperties,
}

impl NodeScanExec {
    pub fn new(
        type_name: String,
        variable_name: String,
        output_schema: SchemaRef,
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
            storage,
            properties,
        }
    }
}

impl DisplayAs for NodeScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "NodeScanExec: ${}: {}",
            self.variable_name, self.type_name
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
        let batch = self.storage.get_all_nodes(&self.type_name)
            .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?;

        let result_batch = match batch {
            Some(b) => {
                let columns: Vec<ArrayRef> = b.columns().to_vec();
                let fields: Vec<Field> = b.schema().fields().iter().map(|f| f.as_ref().clone()).collect();
                let struct_array = StructArray::new(fields.into(), columns, None);
                RecordBatch::try_new(
                    self.output_schema.clone(),
                    vec![Arc::new(struct_array) as ArrayRef],
                )?
            }
            None => RecordBatch::new_empty(self.output_schema.clone()),
        };

        Ok(Box::pin(MemoryStream::try_new(
            vec![result_batch],
            self.output_schema.clone(),
            None,
        )?))
    }
}
