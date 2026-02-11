use std::any::Any;
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray, StructArray,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::common::{Column, Result, ScalarValue};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::DataFusionError;
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::{Operator, TableProviderFilterPushDown};
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use datafusion::prelude::Expr;
use futures::StreamExt;
use lance::Dataset;
use tracing::debug;

use crate::query::ast::{CompOp, Literal};
use crate::store::graph::GraphStorage;

#[derive(Debug, Clone)]
pub struct NodeScanPredicate {
    pub property: String,
    pub op: CompOp,
    pub literal: Literal,
    pub index_eligible: bool,
}

/// TableProvider for a single node type. Registers with DataFusion as a table.
#[derive(Debug)]
pub struct NodeTypeTable {
    pub type_name: String,
    pub variable_name: String,
    pub struct_schema: SchemaRef,
    pub storage: Arc<GraphStorage>,
}

impl NodeTypeTable {
    pub fn new(type_name: String, variable_name: String, storage: Arc<GraphStorage>) -> Self {
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

    fn scan_filter_from_expr(&self, expr: &Expr) -> Option<NodeScanPredicate> {
        fn comp_from_operator(op: Operator) -> Option<CompOp> {
            match op {
                Operator::Eq => Some(CompOp::Eq),
                Operator::NotEq => Some(CompOp::Ne),
                Operator::Gt => Some(CompOp::Gt),
                Operator::Lt => Some(CompOp::Lt),
                Operator::GtEq => Some(CompOp::Ge),
                Operator::LtEq => Some(CompOp::Le),
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

        fn literal_from_scalar(value: &ScalarValue) -> Option<Literal> {
            match value {
                ScalarValue::Boolean(Some(v)) => Some(Literal::Bool(*v)),
                ScalarValue::Int8(Some(v)) => Some(Literal::Integer(*v as i64)),
                ScalarValue::Int16(Some(v)) => Some(Literal::Integer(*v as i64)),
                ScalarValue::Int32(Some(v)) => Some(Literal::Integer(*v as i64)),
                ScalarValue::Int64(Some(v)) => Some(Literal::Integer(*v)),
                ScalarValue::UInt8(Some(v)) => Some(Literal::Integer(*v as i64)),
                ScalarValue::UInt16(Some(v)) => Some(Literal::Integer(*v as i64)),
                ScalarValue::UInt32(Some(v)) => Some(Literal::Integer(*v as i64)),
                ScalarValue::UInt64(Some(v)) => i64::try_from(*v).ok().map(Literal::Integer),
                ScalarValue::Float32(Some(v)) => Some(Literal::Float(*v as f64)),
                ScalarValue::Float64(Some(v)) => Some(Literal::Float(*v)),
                ScalarValue::Utf8(Some(v))
                | ScalarValue::Utf8View(Some(v))
                | ScalarValue::LargeUtf8(Some(v)) => Some(Literal::String(v.clone())),
                _ => None,
            }
        }

        fn literal_from_expr(expr: &Expr) -> Option<Literal> {
            match expr {
                Expr::Literal(value, _) => literal_from_scalar(value),
                Expr::Cast(cast) => literal_from_expr(&cast.expr),
                Expr::TryCast(cast) => literal_from_expr(&cast.expr),
                _ => None,
            }
        }

        fn property_from_column(column: &Column, variable_name: &str) -> Option<String> {
            if let Some(relation) = &column.relation {
                if relation.to_string() == variable_name {
                    return Some(column.name.clone());
                }
                return None;
            }

            if column.name == variable_name {
                return None;
            }
            if let Some((var, prop)) = column.name.split_once('.') {
                if var == variable_name {
                    return Some(prop.to_string());
                }
                return None;
            }

            Some(column.name.clone())
        }

        match expr {
            Expr::BinaryExpr(binary) => {
                let op = comp_from_operator(binary.op)?;
                match (binary.left.as_ref(), binary.right.as_ref()) {
                    (Expr::Column(col), rhs) => {
                        let property = property_from_column(col, &self.variable_name)?;
                        Some(NodeScanPredicate {
                            index_eligible: self.is_index_eligible(&property, op),
                            property,
                            op,
                            literal: literal_from_expr(rhs)?,
                        })
                    }
                    (lhs, Expr::Column(col)) => {
                        let property = property_from_column(col, &self.variable_name)?;
                        let op = flip_comp_op(op);
                        Some(NodeScanPredicate {
                            index_eligible: self.is_index_eligible(&property, op),
                            property,
                            op,
                            literal: literal_from_expr(lhs)?,
                        })
                    }
                    _ => None,
                }
            }
            _ => None,
        }
    }

    fn is_index_eligible(&self, property: &str, op: CompOp) -> bool {
        matches!(
            op,
            CompOp::Eq | CompOp::Gt | CompOp::Lt | CompOp::Ge | CompOp::Le
        ) && self
            .storage
            .catalog
            .node_types
            .get(&self.type_name)
            .map(|node| node.indexed_properties.contains(property))
            .unwrap_or(false)
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

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|filter| {
                if self.scan_filter_from_expr(filter).is_some() {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Table schema is a single struct column for the bound variable.
        // Accept projections that keep column 0 and ignore identity projections.
        if let Some(projection) = projection {
            if !projection.iter().all(|idx| *idx == 0) {
                return Err(datafusion::error::DataFusionError::Plan(
                    "NodeTypeTable only supports projecting the variable struct column".to_string(),
                ));
            }
        }

        let pushdown_filters = filters
            .iter()
            .filter_map(|expr| self.scan_filter_from_expr(expr))
            .collect();

        Ok(Arc::new(NodeScanExec::new(
            self.type_name.clone(),
            self.variable_name.clone(),
            self.struct_schema.clone(),
            pushdown_filters,
            limit,
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
    pushdown_filters: Vec<NodeScanPredicate>,
    limit: Option<usize>,
    storage: Arc<GraphStorage>,
    properties: PlanProperties,
}

impl NodeScanExec {
    pub fn new(
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
