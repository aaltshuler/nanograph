use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, AsArray, RecordBatch, UInt64Builder};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, UInt64Type};

use crate::catalog::Catalog;
use crate::error::{NanoError, Result};
use crate::types::NodeId;

use super::csr::CsrIndex;

#[derive(Debug, Clone)]
pub struct GraphStorage {
    pub catalog: Catalog,
    pub node_segments: HashMap<String, NodeSegment>,
    pub edge_segments: HashMap<String, EdgeSegment>,
    next_node_id: u64,
    next_edge_id: u64,
}

#[derive(Debug, Clone)]
pub struct NodeSegment {
    pub type_name: String,
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
    pub id_to_row: HashMap<u64, (usize, usize)>, // id -> (batch_idx, row_idx)
    pub next_local_id: u64,
}

#[derive(Debug, Clone)]
pub struct EdgeSegment {
    pub type_name: String,
    pub schema: SchemaRef,
    pub src_ids: Vec<u64>,
    pub dst_ids: Vec<u64>,
    pub edge_ids: Vec<u64>,
    pub batches: Vec<RecordBatch>,
    pub csr: Option<CsrIndex>,
    pub csc: Option<CsrIndex>,
}

impl GraphStorage {
    pub fn new(catalog: Catalog) -> Self {
        let mut node_segments = HashMap::new();
        let mut edge_segments = HashMap::new();

        for (name, node_type) in &catalog.node_types {
            node_segments.insert(
                name.clone(),
                NodeSegment {
                    type_name: name.clone(),
                    schema: node_type.arrow_schema.clone(),
                    batches: Vec::new(),
                    id_to_row: HashMap::new(),
                    next_local_id: 0,
                },
            );
        }

        for (name, edge_type) in &catalog.edge_types {
            let mut fields = vec![
                Field::new("id", DataType::UInt64, false),
                Field::new("src", DataType::UInt64, false),
                Field::new("dst", DataType::UInt64, false),
            ];
            for (prop_name, prop_type) in &edge_type.properties {
                fields.push(Field::new(
                    prop_name,
                    prop_type.scalar.to_arrow(),
                    prop_type.nullable,
                ));
            }
            let schema = Arc::new(Schema::new(fields));

            edge_segments.insert(
                name.clone(),
                EdgeSegment {
                    type_name: name.clone(),
                    schema,
                    src_ids: Vec::new(),
                    dst_ids: Vec::new(),
                    edge_ids: Vec::new(),
                    batches: Vec::new(),
                    csr: None,
                    csc: None,
                },
            );
        }

        GraphStorage {
            catalog,
            node_segments,
            edge_segments,
            next_node_id: 0,
            next_edge_id: 0,
        }
    }

    /// Insert nodes of a given type. The batch should NOT contain an `id` column;
    /// IDs will be assigned automatically. Returns the assigned node IDs.
    pub fn insert_nodes(
        &mut self,
        type_name: &str,
        batch: RecordBatch,
    ) -> Result<Vec<NodeId>> {
        let segment = self.node_segments.get_mut(type_name).ok_or_else(|| {
            NanoError::Storage(format!("unknown node type: {}", type_name))
        })?;

        let num_rows = batch.num_rows();
        let mut ids = Vec::with_capacity(num_rows);

        // Assign IDs
        let mut id_builder = UInt64Builder::with_capacity(num_rows);
        for _ in 0..num_rows {
            let id = self.next_node_id;
            self.next_node_id += 1;
            id_builder.append_value(id);
            ids.push(id);
        }
        let id_array = Arc::new(id_builder.finish());

        // Build new batch with id column prepended
        let mut columns: Vec<Arc<dyn Array>> = vec![id_array];
        for col in batch.columns() {
            columns.push(col.clone());
        }
        let new_batch = RecordBatch::try_new(segment.schema.clone(), columns)
            .map_err(|e| NanoError::Storage(format!("failed to create batch: {}", e)))?;

        let batch_idx = segment.batches.len();
        for (row_idx, &id) in ids.iter().enumerate() {
            segment.id_to_row.insert(id, (batch_idx, row_idx));
        }
        segment.batches.push(new_batch);

        Ok(ids)
    }

    /// Insert edges of a given type.
    /// `src_ids` and `dst_ids` are the node IDs of the source and destination nodes.
    /// `props_batch` contains edge property columns (optional, may have 0 columns).
    pub fn insert_edges(
        &mut self,
        type_name: &str,
        src_ids: &[u64],
        dst_ids: &[u64],
        props_batch: Option<RecordBatch>,
    ) -> Result<Vec<u64>> {
        if src_ids.len() != dst_ids.len() {
            return Err(NanoError::Storage(
                "src_ids and dst_ids must have the same length".to_string(),
            ));
        }

        let segment = self.edge_segments.get_mut(type_name).ok_or_else(|| {
            NanoError::Storage(format!("unknown edge type: {}", type_name))
        })?;

        let num_edges = src_ids.len();
        let mut edge_ids = Vec::with_capacity(num_edges);

        for _ in 0..num_edges {
            let eid = self.next_edge_id;
            self.next_edge_id += 1;
            edge_ids.push(eid);
        }

        segment.src_ids.extend_from_slice(src_ids);
        segment.dst_ids.extend_from_slice(dst_ids);
        segment.edge_ids.extend_from_slice(&edge_ids);

        if let Some(batch) = props_batch {
            segment.batches.push(batch);
        }

        Ok(edge_ids)
    }

    /// Build CSR and CSC indices for all edge types.
    pub fn build_indices(&mut self) -> Result<()> {
        // Find the max node ID to determine CSR size
        let max_node_id = self.next_node_id;

        for segment in self.edge_segments.values_mut() {
            let num_edges = segment.src_ids.len();

            // Build CSR (outgoing edges)
            let mut out_edges: Vec<(u64, u64, u64)> = Vec::with_capacity(num_edges);
            for i in 0..num_edges {
                out_edges.push((segment.src_ids[i], segment.dst_ids[i], segment.edge_ids[i]));
            }
            segment.csr = Some(CsrIndex::build(max_node_id as usize, &mut out_edges));

            // Build CSC (incoming edges) — swap src/dst
            let mut in_edges: Vec<(u64, u64, u64)> = Vec::with_capacity(num_edges);
            for i in 0..num_edges {
                in_edges.push((segment.dst_ids[i], segment.src_ids[i], segment.edge_ids[i]));
            }
            segment.csc = Some(CsrIndex::build(max_node_id as usize, &mut in_edges));
        }

        Ok(())
    }

    /// Get a node's data by ID and type. Returns the row as a slice of the RecordBatch.
    pub fn get_node_batch_and_row(
        &self,
        type_name: &str,
        node_id: u64,
    ) -> Option<(&RecordBatch, usize)> {
        let segment = self.node_segments.get(type_name)?;
        let &(batch_idx, row_idx) = segment.id_to_row.get(&node_id)?;
        Some((&segment.batches[batch_idx], row_idx))
    }

    /// Get a struct schema for a node type (wraps all fields in a Struct).
    pub fn node_struct_schema(&self, type_name: &str) -> Option<SchemaRef> {
        self.node_segments.get(type_name).map(|s| s.schema.clone())
    }

    /// Get the full RecordBatch for all nodes of a type.
    /// Concatenates all batches.
    pub fn get_all_nodes(&self, type_name: &str) -> Result<Option<RecordBatch>> {
        let segment = self.node_segments.get(type_name);
        match segment {
            None => Ok(None),
            Some(seg) => {
                if seg.batches.is_empty() {
                    return Ok(None);
                }
                if seg.batches.len() == 1 {
                    return Ok(Some(seg.batches[0].clone()));
                }
                let batch = arrow::compute::concat_batches(&seg.schema, &seg.batches)
                    .map_err(|e| NanoError::Storage(format!("concat error: {}", e)))?;
                Ok(Some(batch))
            }
        }
    }
    /// Load a pre-ID'd node batch (has id column already). Does not auto-assign IDs.
    /// Used when restoring from persistence.
    pub fn load_node_batch(&mut self, type_name: &str, batch: RecordBatch) -> Result<()> {
        let segment = self.node_segments.get_mut(type_name).ok_or_else(|| {
            NanoError::Storage(format!("unknown node type: {}", type_name))
        })?;

        // Extract IDs from the batch to build id_to_row
        let id_col = batch
            .column_by_name("id")
            .ok_or_else(|| NanoError::Storage("batch missing 'id' column".to_string()))?;
        let id_arr = id_col.as_primitive::<UInt64Type>();

        let batch_idx = segment.batches.len();
        for row_idx in 0..batch.num_rows() {
            let id = id_arr.value(row_idx);
            segment.id_to_row.insert(id, (batch_idx, row_idx));
            // Track max ID to keep next_node_id correct
            if id >= self.next_node_id {
                self.next_node_id = id + 1;
            }
        }
        segment.batches.push(batch);

        Ok(())
    }

    /// Load edge data from a combined batch (edge_id, src, dst, ...props).
    /// Extracts vectors and optional property columns.
    pub fn load_edge_batch(&mut self, type_name: &str, batch: RecordBatch) -> Result<()> {
        let segment = self.edge_segments.get_mut(type_name).ok_or_else(|| {
            NanoError::Storage(format!("unknown edge type: {}", type_name))
        })?;

        let id_col = batch
            .column_by_name("id")
            .ok_or_else(|| NanoError::Storage("edge batch missing 'id' column".to_string()))?;
        let src_col = batch
            .column_by_name("src")
            .ok_or_else(|| NanoError::Storage("edge batch missing 'src' column".to_string()))?;
        let dst_col = batch
            .column_by_name("dst")
            .ok_or_else(|| NanoError::Storage("edge batch missing 'dst' column".to_string()))?;

        let id_arr = id_col.as_primitive::<UInt64Type>();
        let src_arr = src_col.as_primitive::<UInt64Type>();
        let dst_arr = dst_col.as_primitive::<UInt64Type>();

        for i in 0..batch.num_rows() {
            let eid = id_arr.value(i);
            segment.edge_ids.push(eid);
            segment.src_ids.push(src_arr.value(i));
            segment.dst_ids.push(dst_arr.value(i));
            if eid >= self.next_edge_id {
                self.next_edge_id = eid + 1;
            }
        }

        // Extract property columns (everything after id, src, dst)
        let batch_schema = batch.schema();
        let prop_col_indices: Vec<usize> = (0..batch.num_columns())
            .filter(|&i| {
                let name = batch_schema.field(i).name();
                name != "id" && name != "src" && name != "dst"
            })
            .collect();

        if !prop_col_indices.is_empty() {
            let prop_fields: Vec<Field> = prop_col_indices
                .iter()
                .map(|&i| batch.schema().field(i).clone())
                .collect();
            let prop_cols: Vec<Arc<dyn Array>> = prop_col_indices
                .iter()
                .map(|&i| batch.column(i).clone())
                .collect();
            let prop_schema = Arc::new(Schema::new(prop_fields));
            let prop_batch = RecordBatch::try_new(prop_schema, prop_cols)
                .map_err(|e| NanoError::Storage(format!("prop batch error: {}", e)))?;
            segment.batches.push(prop_batch);
        }

        Ok(())
    }

    /// Build a combined edge batch (id, src, dst, ...props) for persistence.
    pub fn edge_batch_for_save(&self, type_name: &str) -> Result<Option<RecordBatch>> {
        let segment = match self.edge_segments.get(type_name) {
            Some(s) => s,
            None => return Ok(None),
        };

        if segment.edge_ids.is_empty() {
            return Ok(None);
        }

        let num_edges = segment.edge_ids.len();
        let id_arr: Arc<dyn Array> = Arc::new(arrow::array::UInt64Array::from(segment.edge_ids.clone()));
        let src_arr: Arc<dyn Array> = Arc::new(arrow::array::UInt64Array::from(segment.src_ids.clone()));
        let dst_arr: Arc<dyn Array> = Arc::new(arrow::array::UInt64Array::from(segment.dst_ids.clone()));

        let mut fields = vec![
            Field::new("id", DataType::UInt64, false),
            Field::new("src", DataType::UInt64, false),
            Field::new("dst", DataType::UInt64, false),
        ];
        let mut columns: Vec<Arc<dyn Array>> = vec![id_arr, src_arr, dst_arr];

        // Concatenate property batches if any
        if !segment.batches.is_empty() {
            let prop_schema = segment.batches[0].schema();
            let prop_batch = if segment.batches.len() == 1 {
                segment.batches[0].clone()
            } else {
                arrow::compute::concat_batches(&prop_schema, &segment.batches)
                    .map_err(|e| NanoError::Storage(format!("concat error: {}", e)))?
            };

            // Verify row counts match
            if prop_batch.num_rows() != num_edges {
                return Err(NanoError::Storage(format!(
                    "edge property batch has {} rows but {} edges",
                    prop_batch.num_rows(),
                    num_edges
                )));
            }

            for (i, field) in prop_schema.fields().iter().enumerate() {
                fields.push(field.as_ref().clone());
                columns.push(prop_batch.column(i).clone());
            }
        }

        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema, columns)
            .map_err(|e| NanoError::Storage(format!("batch error: {}", e)))?;

        Ok(Some(batch))
    }

    pub fn set_next_node_id(&mut self, id: u64) {
        self.next_node_id = id;
    }

    pub fn set_next_edge_id(&mut self, id: u64) {
        self.next_edge_id = id;
    }

    pub fn next_node_id(&self) -> u64 {
        self.next_node_id
    }

    pub fn next_edge_id(&self) -> u64 {
        self.next_edge_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::build_catalog;
    use crate::schema::parser::parse_schema;
    use arrow::array::StringArray;

    fn test_storage() -> GraphStorage {
        let schema = parse_schema(
            r#"
node Person {
    name: String
    age: I32?
}
node Company {
    name: String
}
edge Knows: Person -> Person {
    since: Date?
}
edge WorksAt: Person -> Company
"#,
        )
        .unwrap();
        let catalog = build_catalog(&schema).unwrap();
        GraphStorage::new(catalog)
    }

    #[test]
    fn test_insert_nodes() {
        let mut storage = test_storage();

        let person_schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int32, true),
        ]));

        let batch = RecordBatch::try_new(
            person_schema,
            vec![
                Arc::new(StringArray::from(vec!["Alice", "Bob"])),
                Arc::new(arrow::array::Int32Array::from(vec![Some(30), Some(25)])),
            ],
        )
        .unwrap();

        let ids = storage.insert_nodes("Person", batch).unwrap();
        assert_eq!(ids, vec![0, 1]);

        // Verify we can look up nodes
        let (batch, row) = storage.get_node_batch_and_row("Person", 0).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(row, 0);
    }

    #[test]
    fn test_insert_edges_and_build_csr() {
        let mut storage = test_storage();

        // Insert people
        let person_schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(
            person_schema,
            vec![
                Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
                Arc::new(arrow::array::Int32Array::from(vec![
                    Some(30),
                    Some(25),
                    Some(35),
                ])),
            ],
        )
        .unwrap();
        let ids = storage.insert_nodes("Person", batch).unwrap();

        // Insert edges: Alice->Bob, Alice->Charlie
        storage
            .insert_edges("Knows", &[ids[0], ids[0]], &[ids[1], ids[2]], None)
            .unwrap();

        // Build indices
        storage.build_indices().unwrap();

        // Check CSR
        let edge_seg = &storage.edge_segments["Knows"];
        let csr = edge_seg.csr.as_ref().unwrap();
        assert_eq!(csr.neighbors(ids[0]), &[ids[1], ids[2]]);
        assert_eq!(csr.neighbors(ids[1]), &[] as &[u64]);

        // Check CSC (incoming)
        let csc = edge_seg.csc.as_ref().unwrap();
        assert_eq!(csc.neighbors(ids[1]), &[ids[0]]); // Bob's incoming: Alice
        assert_eq!(csc.neighbors(ids[2]), &[ids[0]]); // Charlie's incoming: Alice
    }

    #[test]
    fn test_get_all_nodes() {
        let mut storage = test_storage();
        let person_schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(
            person_schema,
            vec![
                Arc::new(StringArray::from(vec!["Alice"])),
                Arc::new(arrow::array::Int32Array::from(vec![Some(30)])),
            ],
        )
        .unwrap();
        storage.insert_nodes("Person", batch).unwrap();

        let all = storage.get_all_nodes("Person").unwrap().unwrap();
        assert_eq!(all.num_rows(), 1);
        assert_eq!(all.num_columns(), 3); // id, name, age
    }
}
