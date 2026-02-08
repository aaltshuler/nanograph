use pest::Parser;
use pest_derive::Parser;

use crate::error::{NanoError, Result};
use crate::types::{PropType, ScalarType};

use super::ast::*;

#[derive(Parser)]
#[grammar = "schema/schema.pest"]
struct SchemaParser;

pub fn parse_schema(input: &str) -> Result<SchemaFile> {
    let pairs = SchemaParser::parse(Rule::schema_file, input)
        .map_err(|e| NanoError::Parse(e.to_string()))?;

    let mut declarations = Vec::new();
    for pair in pairs {
        match pair.as_rule() {
            Rule::schema_file => {
                for inner in pair.into_inner() {
                    if let Rule::schema_decl = inner.as_rule() {
                        declarations.push(parse_schema_decl(inner)?);
                    }
                }
            }
            _ => {}
        }
    }
    Ok(SchemaFile { declarations })
}

fn parse_schema_decl(pair: pest::iterators::Pair<Rule>) -> Result<SchemaDecl> {
    let inner = pair.into_inner().next().unwrap();
    match inner.as_rule() {
        Rule::node_decl => Ok(SchemaDecl::Node(parse_node_decl(inner)?)),
        Rule::edge_decl => Ok(SchemaDecl::Edge(parse_edge_decl(inner)?)),
        _ => Err(NanoError::Parse(format!(
            "unexpected rule: {:?}",
            inner.as_rule()
        ))),
    }
}

fn parse_node_decl(pair: pest::iterators::Pair<Rule>) -> Result<NodeDecl> {
    let mut inner = pair.into_inner();
    let name = inner.next().unwrap().as_str().to_string();

    let mut parent = None;
    let mut properties = Vec::new();

    for item in inner {
        match item.as_rule() {
            Rule::type_name => {
                parent = Some(item.as_str().to_string());
            }
            Rule::prop_decl => {
                properties.push(parse_prop_decl(item)?);
            }
            _ => {}
        }
    }

    Ok(NodeDecl {
        name,
        parent,
        properties,
    })
}

fn parse_edge_decl(pair: pest::iterators::Pair<Rule>) -> Result<EdgeDecl> {
    let mut inner = pair.into_inner();
    let name = inner.next().unwrap().as_str().to_string();
    let from_type = inner.next().unwrap().as_str().to_string();
    let to_type = inner.next().unwrap().as_str().to_string();

    let mut properties = Vec::new();
    for item in inner {
        if let Rule::prop_decl = item.as_rule() {
            properties.push(parse_prop_decl(item)?);
        }
    }

    Ok(EdgeDecl {
        name,
        from_type,
        to_type,
        properties,
    })
}

fn parse_prop_decl(pair: pest::iterators::Pair<Rule>) -> Result<PropDecl> {
    let mut inner = pair.into_inner();
    let name = inner.next().unwrap().as_str().to_string();
    let type_ref = inner.next().unwrap();
    let prop_type = parse_type_ref(type_ref)?;

    let mut annotations = Vec::new();
    for item in inner {
        if let Rule::annotation = item.as_rule() {
            annotations.push(parse_annotation(item)?);
        }
    }

    Ok(PropDecl {
        name,
        prop_type,
        annotations,
    })
}

fn parse_type_ref(pair: pest::iterators::Pair<Rule>) -> Result<PropType> {
    let text = pair.as_str();
    let nullable = text.ends_with('?');

    let inner = pair.into_inner().next().unwrap();
    let scalar = ScalarType::from_str_name(inner.as_str())
        .ok_or_else(|| NanoError::Parse(format!("unknown type: {}", inner.as_str())))?;

    Ok(PropType { scalar, nullable })
}

fn parse_annotation(pair: pest::iterators::Pair<Rule>) -> Result<Annotation> {
    let mut inner = pair.into_inner();
    let name = inner.next().unwrap().as_str().to_string();
    let value = inner.next().map(|p| {
        let s = p.as_str();
        if s.starts_with('"') && s.ends_with('"') {
            s[1..s.len() - 1].to_string()
        } else {
            s.to_string()
        }
    });

    Ok(Annotation { name, value })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_basic_schema() {
        let input = r#"
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

edge WorksAt: Person -> Company {
    title: String?
}
"#;
        let schema = parse_schema(input).unwrap();
        assert_eq!(schema.declarations.len(), 4);

        // Check Person node
        match &schema.declarations[0] {
            SchemaDecl::Node(n) => {
                assert_eq!(n.name, "Person");
                assert!(n.parent.is_none());
                assert_eq!(n.properties.len(), 2);
                assert_eq!(n.properties[0].name, "name");
                assert!(!n.properties[0].prop_type.nullable);
                assert_eq!(n.properties[1].name, "age");
                assert!(n.properties[1].prop_type.nullable);
            }
            _ => panic!("expected Node"),
        }

        // Check Knows edge
        match &schema.declarations[2] {
            SchemaDecl::Edge(e) => {
                assert_eq!(e.name, "Knows");
                assert_eq!(e.from_type, "Person");
                assert_eq!(e.to_type, "Person");
                assert_eq!(e.properties.len(), 1);
            }
            _ => panic!("expected Edge"),
        }
    }

    #[test]
    fn test_parse_inheritance() {
        let input = r#"
node Person {
    name: String
}
node Employee : Person {
    employee_id: String
}
"#;
        let schema = parse_schema(input).unwrap();
        match &schema.declarations[1] {
            SchemaDecl::Node(n) => {
                assert_eq!(n.name, "Employee");
                assert_eq!(n.parent.as_deref(), Some("Person"));
            }
            _ => panic!("expected Node"),
        }
    }

    #[test]
    fn test_parse_annotation() {
        let input = r#"
node Person {
    name: String @unique
    id: U64 @key
}
"#;
        let schema = parse_schema(input).unwrap();
        match &schema.declarations[0] {
            SchemaDecl::Node(n) => {
                assert_eq!(n.properties[0].annotations.len(), 1);
                assert_eq!(n.properties[0].annotations[0].name, "unique");
                assert_eq!(n.properties[1].annotations[0].name, "key");
            }
            _ => panic!("expected Node"),
        }
    }

    #[test]
    fn test_parse_edge_no_body() {
        let input = "edge WorksAt: Person -> Company\n";
        let schema = parse_schema(input).unwrap();
        match &schema.declarations[0] {
            SchemaDecl::Edge(e) => {
                assert_eq!(e.name, "WorksAt");
                assert!(e.properties.is_empty());
            }
            _ => panic!("expected Edge"),
        }
    }

    #[test]
    fn test_parse_error() {
        let input = "node { }"; // missing type name
        assert!(parse_schema(input).is_err());
    }
}
