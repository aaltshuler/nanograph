use crate::types::PropType;

#[derive(Debug, Clone)]
pub struct SchemaFile {
    pub declarations: Vec<SchemaDecl>,
}

#[derive(Debug, Clone)]
pub enum SchemaDecl {
    Node(NodeDecl),
    Edge(EdgeDecl),
}

#[derive(Debug, Clone)]
pub struct NodeDecl {
    pub name: String,
    pub parent: Option<String>,
    pub properties: Vec<PropDecl>,
}

#[derive(Debug, Clone)]
pub struct EdgeDecl {
    pub name: String,
    pub from_type: String,
    pub to_type: String,
    pub properties: Vec<PropDecl>,
}

#[derive(Debug, Clone)]
pub struct PropDecl {
    pub name: String,
    pub prop_type: PropType,
    pub annotations: Vec<Annotation>,
}

#[derive(Debug, Clone)]
pub struct Annotation {
    pub name: String,
    pub value: Option<String>,
}
