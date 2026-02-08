use pest::Parser;
use pest_derive::Parser;

use crate::error::{NanoError, Result};

use super::ast::*;

#[derive(Parser)]
#[grammar = "query/query.pest"]
struct QueryParser;

pub fn parse_query(input: &str) -> Result<QueryFile> {
    let pairs = QueryParser::parse(Rule::query_file, input)
        .map_err(|e| NanoError::Parse(e.to_string()))?;

    let mut queries = Vec::new();
    for pair in pairs {
        if let Rule::query_file = pair.as_rule() {
            for inner in pair.into_inner() {
                if let Rule::query_decl = inner.as_rule() {
                    queries.push(parse_query_decl(inner)?);
                }
            }
        }
    }
    Ok(QueryFile { queries })
}

fn parse_query_decl(pair: pest::iterators::Pair<Rule>) -> Result<QueryDecl> {
    let mut inner = pair.into_inner();
    let name = inner.next().unwrap().as_str().to_string();

    let mut params = Vec::new();
    let mut match_clause = Vec::new();
    let mut return_clause = Vec::new();
    let mut order_clause = Vec::new();
    let mut limit = None;

    for item in inner {
        match item.as_rule() {
            Rule::param_list => {
                for p in item.into_inner() {
                    if let Rule::param = p.as_rule() {
                        params.push(parse_param(p)?);
                    }
                }
            }
            Rule::match_clause => {
                for c in item.into_inner() {
                    if let Rule::clause = c.as_rule() {
                        match_clause.push(parse_clause(c)?);
                    }
                }
            }
            Rule::return_clause => {
                for proj in item.into_inner() {
                    if let Rule::projection = proj.as_rule() {
                        return_clause.push(parse_projection(proj)?);
                    }
                }
            }
            Rule::order_clause => {
                for ord in item.into_inner() {
                    if let Rule::ordering = ord.as_rule() {
                        order_clause.push(parse_ordering(ord)?);
                    }
                }
            }
            Rule::limit_clause => {
                let int_pair = item.into_inner().next().unwrap();
                limit = Some(int_pair.as_str().parse::<u64>().map_err(|e| {
                    NanoError::Parse(format!("invalid limit: {}", e))
                })?);
            }
            _ => {}
        }
    }

    Ok(QueryDecl {
        name,
        params,
        match_clause,
        return_clause,
        order_clause,
        limit,
    })
}

fn parse_param(pair: pest::iterators::Pair<Rule>) -> Result<Param> {
    let mut inner = pair.into_inner();
    let var = inner.next().unwrap().as_str();
    let name = var.strip_prefix('$').unwrap_or(var).to_string();
    let type_ref = inner.next().unwrap();
    let type_str = type_ref.as_str();
    let nullable = type_str.ends_with('?');
    let base = type_ref.into_inner().next().unwrap().as_str().to_string();

    Ok(Param {
        name,
        type_name: base,
        nullable,
    })
}

fn parse_clause(pair: pest::iterators::Pair<Rule>) -> Result<Clause> {
    let inner = pair.into_inner().next().unwrap();
    match inner.as_rule() {
        Rule::binding => Ok(Clause::Binding(parse_binding(inner)?)),
        Rule::traversal => Ok(Clause::Traversal(parse_traversal(inner)?)),
        Rule::filter => Ok(Clause::Filter(parse_filter(inner)?)),
        Rule::negation => {
            let mut clauses = Vec::new();
            for c in inner.into_inner() {
                if let Rule::clause = c.as_rule() {
                    clauses.push(parse_clause(c)?);
                }
            }
            Ok(Clause::Negation(clauses))
        }
        _ => Err(NanoError::Parse(format!(
            "unexpected clause rule: {:?}",
            inner.as_rule()
        ))),
    }
}

fn parse_binding(pair: pest::iterators::Pair<Rule>) -> Result<Binding> {
    let mut inner = pair.into_inner();
    let var = inner.next().unwrap().as_str();
    let variable = var.strip_prefix('$').unwrap_or(var).to_string();
    let type_name = inner.next().unwrap().as_str().to_string();

    let mut prop_matches = Vec::new();
    for item in inner {
        if let Rule::prop_match_list = item.as_rule() {
            for pm in item.into_inner() {
                if let Rule::prop_match = pm.as_rule() {
                    prop_matches.push(parse_prop_match(pm)?);
                }
            }
        }
    }

    Ok(Binding {
        variable,
        type_name,
        prop_matches,
    })
}

fn parse_prop_match(pair: pest::iterators::Pair<Rule>) -> Result<PropMatch> {
    let mut inner = pair.into_inner();
    let prop_name = inner.next().unwrap().as_str().to_string();
    let value_pair = inner.next().unwrap();
    let value_inner = value_pair.into_inner().next().unwrap();

    let value = match value_inner.as_rule() {
        Rule::variable => {
            let v = value_inner.as_str();
            MatchValue::Variable(v.strip_prefix('$').unwrap_or(v).to_string())
        }
        Rule::literal => MatchValue::Literal(parse_literal(value_inner)?),
        _ => {
            return Err(NanoError::Parse(format!(
                "unexpected match value: {:?}",
                value_inner.as_rule()
            )))
        }
    };

    Ok(PropMatch { prop_name, value })
}

fn parse_traversal(pair: pest::iterators::Pair<Rule>) -> Result<Traversal> {
    let mut inner = pair.into_inner();
    let src_var = inner.next().unwrap().as_str();
    let src = src_var.strip_prefix('$').unwrap_or(src_var).to_string();
    let edge_name = inner.next().unwrap().as_str().to_string();
    let dst_var = inner.next().unwrap().as_str();
    let dst = dst_var.strip_prefix('$').unwrap_or(dst_var).to_string();

    Ok(Traversal {
        src,
        edge_name,
        dst,
    })
}

fn parse_filter(pair: pest::iterators::Pair<Rule>) -> Result<Filter> {
    let mut inner = pair.into_inner();
    let left = parse_expr(inner.next().unwrap())?;
    let op = parse_comp_op(inner.next().unwrap())?;
    let right = parse_expr(inner.next().unwrap())?;

    Ok(Filter { left, op, right })
}

fn parse_expr(pair: pest::iterators::Pair<Rule>) -> Result<Expr> {
    let inner = pair.into_inner().next().unwrap();
    match inner.as_rule() {
        Rule::prop_access => {
            let mut parts = inner.into_inner();
            let var = parts.next().unwrap().as_str();
            let variable = var.strip_prefix('$').unwrap_or(var).to_string();
            let property = parts.next().unwrap().as_str().to_string();
            Ok(Expr::PropAccess { variable, property })
        }
        Rule::variable => {
            let v = inner.as_str();
            Ok(Expr::Variable(
                v.strip_prefix('$').unwrap_or(v).to_string(),
            ))
        }
        Rule::literal => Ok(Expr::Literal(parse_literal(inner)?)),
        Rule::agg_call => {
            let mut parts = inner.into_inner();
            let func = match parts.next().unwrap().as_str() {
                "count" => AggFunc::Count,
                "sum" => AggFunc::Sum,
                "avg" => AggFunc::Avg,
                "min" => AggFunc::Min,
                "max" => AggFunc::Max,
                other => {
                    return Err(NanoError::Parse(format!(
                        "unknown aggregate: {}",
                        other
                    )))
                }
            };
            let arg = parse_expr(parts.next().unwrap())?;
            Ok(Expr::Aggregate {
                func,
                arg: Box::new(arg),
            })
        }
        Rule::ident => Ok(Expr::AliasRef(inner.as_str().to_string())),
        _ => Err(NanoError::Parse(format!(
            "unexpected expr rule: {:?}",
            inner.as_rule()
        ))),
    }
}

fn parse_comp_op(pair: pest::iterators::Pair<Rule>) -> Result<CompOp> {
    match pair.as_str() {
        "=" => Ok(CompOp::Eq),
        "!=" => Ok(CompOp::Ne),
        ">" => Ok(CompOp::Gt),
        "<" => Ok(CompOp::Lt),
        ">=" => Ok(CompOp::Ge),
        "<=" => Ok(CompOp::Le),
        other => Err(NanoError::Parse(format!("unknown operator: {}", other))),
    }
}

fn parse_literal(pair: pest::iterators::Pair<Rule>) -> Result<Literal> {
    let inner = pair.into_inner().next().unwrap();
    match inner.as_rule() {
        Rule::string_lit => {
            let s = inner.as_str();
            Ok(Literal::String(s[1..s.len() - 1].to_string()))
        }
        Rule::integer => {
            let n: i64 = inner.as_str().parse().map_err(|e| {
                NanoError::Parse(format!("invalid integer: {}", e))
            })?;
            Ok(Literal::Integer(n))
        }
        Rule::float_lit => {
            let f: f64 = inner.as_str().parse().map_err(|e| {
                NanoError::Parse(format!("invalid float: {}", e))
            })?;
            Ok(Literal::Float(f))
        }
        Rule::bool_lit => {
            let b = inner.as_str() == "true";
            Ok(Literal::Bool(b))
        }
        _ => Err(NanoError::Parse(format!(
            "unexpected literal: {:?}",
            inner.as_rule()
        ))),
    }
}

fn parse_projection(pair: pest::iterators::Pair<Rule>) -> Result<Projection> {
    let mut inner = pair.into_inner();
    let expr = parse_expr(inner.next().unwrap())?;
    let alias = inner.next().map(|p| p.as_str().to_string());

    Ok(Projection { expr, alias })
}

fn parse_ordering(pair: pest::iterators::Pair<Rule>) -> Result<Ordering> {
    let mut inner = pair.into_inner();
    let expr = parse_expr(inner.next().unwrap())?;
    let descending = inner.next().map_or(false, |p| p.as_str() == "desc");

    Ok(Ordering { expr, descending })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_basic_query() {
        let input = r#"
query get_person($name: String) {
    match {
        $p: Person { name: $name }
    }
    return { $p.name, $p.age }
}
"#;
        let qf = parse_query(input).unwrap();
        assert_eq!(qf.queries.len(), 1);
        let q = &qf.queries[0];
        assert_eq!(q.name, "get_person");
        assert_eq!(q.params.len(), 1);
        assert_eq!(q.params[0].name, "name");
        assert_eq!(q.match_clause.len(), 1);
        assert_eq!(q.return_clause.len(), 2);
    }

    #[test]
    fn test_parse_no_params() {
        let input = r#"
query adults() {
    match {
        $p: Person
        $p.age > 30
    }
    return { $p.name, $p.age }
    order { $p.age desc }
}
"#;
        let qf = parse_query(input).unwrap();
        let q = &qf.queries[0];
        assert_eq!(q.name, "adults");
        assert!(q.params.is_empty());
        assert_eq!(q.match_clause.len(), 2);
        assert_eq!(q.order_clause.len(), 1);
        assert!(q.order_clause[0].descending);
    }

    #[test]
    fn test_parse_traversal() {
        let input = r#"
query friends_of($name: String) {
    match {
        $p: Person { name: $name }
        $p knows $f
    }
    return { $f.name, $f.age }
}
"#;
        let qf = parse_query(input).unwrap();
        let q = &qf.queries[0];
        assert_eq!(q.match_clause.len(), 2);
        match &q.match_clause[1] {
            Clause::Traversal(t) => {
                assert_eq!(t.src, "p");
                assert_eq!(t.edge_name, "knows");
                assert_eq!(t.dst, "f");
            }
            _ => panic!("expected Traversal"),
        }
    }

    #[test]
    fn test_parse_negation() {
        let input = r#"
query unemployed() {
    match {
        $p: Person
        not { $p worksAt $_ }
    }
    return { $p.name }
}
"#;
        let qf = parse_query(input).unwrap();
        let q = &qf.queries[0];
        assert_eq!(q.match_clause.len(), 2);
        match &q.match_clause[1] {
            Clause::Negation(clauses) => {
                assert_eq!(clauses.len(), 1);
                match &clauses[0] {
                    Clause::Traversal(t) => {
                        assert_eq!(t.src, "p");
                        assert_eq!(t.edge_name, "worksAt");
                        assert_eq!(t.dst, "_");
                    }
                    _ => panic!("expected Traversal inside negation"),
                }
            }
            _ => panic!("expected Negation"),
        }
    }

    #[test]
    fn test_parse_aggregation() {
        let input = r#"
query friend_counts() {
    match {
        $p: Person
        $p knows $f
    }
    return {
        $p.name
        count($f) as friends
    }
    order { friends desc }
    limit 20
}
"#;
        let qf = parse_query(input).unwrap();
        let q = &qf.queries[0];
        assert_eq!(q.return_clause.len(), 2);
        match &q.return_clause[1].expr {
            Expr::Aggregate { func, .. } => {
                assert_eq!(*func, AggFunc::Count);
            }
            _ => panic!("expected Aggregate"),
        }
        assert_eq!(q.return_clause[1].alias.as_deref(), Some("friends"));
        assert_eq!(q.limit, Some(20));
    }

    #[test]
    fn test_parse_two_hop() {
        let input = r#"
query friends_of_friends($name: String) {
    match {
        $p: Person { name: $name }
        $p knows $mid
        $mid knows $fof
    }
    return { $fof.name }
}
"#;
        let qf = parse_query(input).unwrap();
        let q = &qf.queries[0];
        assert_eq!(q.match_clause.len(), 3);
    }

    #[test]
    fn test_parse_reverse_traversal() {
        let input = r#"
query employees_of($company: String) {
    match {
        $c: Company { name: $company }
        $p worksAt $c
    }
    return { $p.name }
}
"#;
        let qf = parse_query(input).unwrap();
        let q = &qf.queries[0];
        assert_eq!(q.match_clause.len(), 2);
        match &q.match_clause[1] {
            Clause::Traversal(t) => {
                assert_eq!(t.src, "p");
                assert_eq!(t.edge_name, "worksAt");
                assert_eq!(t.dst, "c");
            }
            _ => panic!("expected Traversal"),
        }
    }

    #[test]
    fn test_parse_multi_query_file() {
        let input = r#"
query q1() {
    match { $p: Person }
    return { $p.name }
}
query q2() {
    match { $c: Company }
    return { $c.name }
}
"#;
        let qf = parse_query(input).unwrap();
        assert_eq!(qf.queries.len(), 2);
    }

    #[test]
    fn test_parse_complex_negation() {
        let input = r#"
query knows_alice_not_bob() {
    match {
        $a: Person { name: "Alice" }
        $b: Person { name: "Bob" }
        $p: Person
        $p knows $a
        not { $p knows $b }
    }
    return { $p.name }
}
"#;
        let qf = parse_query(input).unwrap();
        let q = &qf.queries[0];
        assert_eq!(q.match_clause.len(), 5);
    }

    #[test]
    fn test_parse_filter_string() {
        let input = r#"
query test() {
    match {
        $p: Person
        $p.name != "Bob"
    }
    return { $p.name }
}
"#;
        let qf = parse_query(input).unwrap();
        let q = &qf.queries[0];
        match &q.match_clause[1] {
            Clause::Filter(f) => {
                assert_eq!(f.op, CompOp::Ne);
            }
            _ => panic!("expected Filter"),
        }
    }

    #[test]
    fn test_parse_triangle() {
        let input = r#"
query triangles($name: String) {
    match {
        $a: Person { name: $name }
        $a knows $b
        $b knows $c
        $c knows $a
    }
    return { $b.name, $c.name }
}
"#;
        let qf = parse_query(input).unwrap();
        let q = &qf.queries[0];
        assert_eq!(q.match_clause.len(), 4);
    }

    #[test]
    fn test_parse_avg_aggregation() {
        let input = r#"
query avg_age_by_company() {
    match {
        $p: Person
        $p worksAt $c
    }
    return {
        $c.name
        avg($p.age) as avg_age
        count($p) as headcount
    }
    order { headcount desc }
}
"#;
        let qf = parse_query(input).unwrap();
        let q = &qf.queries[0];
        assert_eq!(q.return_clause.len(), 3);
    }
}
