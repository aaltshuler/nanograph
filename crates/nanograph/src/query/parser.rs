use pest::Parser;
use pest::error::InputLocation;
use pest_derive::Parser;

use crate::error::{NanoError, ParseDiagnostic, Result, SourceSpan};

use super::ast::*;

#[derive(Parser)]
#[grammar = "query/query.pest"]
struct QueryParser;

pub fn parse_query(input: &str) -> Result<QueryFile> {
    parse_query_diagnostic(input).map_err(|e| NanoError::Parse(e.to_string()))
}

pub fn parse_query_diagnostic(input: &str) -> std::result::Result<QueryFile, ParseDiagnostic> {
    let pairs = QueryParser::parse(Rule::query_file, input).map_err(pest_error_to_diagnostic)?;

    let mut queries = Vec::new();
    for pair in pairs {
        if let Rule::query_file = pair.as_rule() {
            for inner in pair.into_inner() {
                if let Rule::query_decl = inner.as_rule() {
                    queries.push(parse_query_decl(inner).map_err(nano_error_to_diagnostic)?);
                }
            }
        }
    }
    Ok(QueryFile { queries })
}

fn pest_error_to_diagnostic(err: pest::error::Error<Rule>) -> ParseDiagnostic {
    let span = match err.location {
        InputLocation::Pos(pos) => Some(SourceSpan::new(pos, pos)),
        InputLocation::Span((start, end)) => Some(SourceSpan::new(start, end)),
    };
    ParseDiagnostic::new(err.to_string(), span)
}

fn nano_error_to_diagnostic(err: NanoError) -> ParseDiagnostic {
    ParseDiagnostic::new(err.to_string(), None)
}

fn parse_query_decl(pair: pest::iterators::Pair<Rule>) -> Result<QueryDecl> {
    let mut inner = pair.into_inner();
    let name = inner.next().unwrap().as_str().to_string();

    let mut params = Vec::new();
    let mut match_clause = Vec::new();
    let mut return_clause = Vec::new();
    let mut order_clause = Vec::new();
    let mut limit = None;
    let mut mutation = None;

    for item in inner {
        match item.as_rule() {
            Rule::param_list => {
                for p in item.into_inner() {
                    if let Rule::param = p.as_rule() {
                        params.push(parse_param(p)?);
                    }
                }
            }
            Rule::query_body => {
                let body = item
                    .into_inner()
                    .next()
                    .ok_or_else(|| NanoError::Parse("query body cannot be empty".to_string()))?;
                match body.as_rule() {
                    Rule::read_query_body => {
                        for section in body.into_inner() {
                            match section.as_rule() {
                                Rule::match_clause => {
                                    for c in section.into_inner() {
                                        if let Rule::clause = c.as_rule() {
                                            match_clause.push(parse_clause(c)?);
                                        }
                                    }
                                }
                                Rule::return_clause => {
                                    for proj in section.into_inner() {
                                        if let Rule::projection = proj.as_rule() {
                                            return_clause.push(parse_projection(proj)?);
                                        }
                                    }
                                }
                                Rule::order_clause => {
                                    for ord in section.into_inner() {
                                        if let Rule::ordering = ord.as_rule() {
                                            order_clause.push(parse_ordering(ord)?);
                                        }
                                    }
                                }
                                Rule::limit_clause => {
                                    let int_pair = section.into_inner().next().unwrap();
                                    limit =
                                        Some(int_pair.as_str().parse::<u64>().map_err(|e| {
                                            NanoError::Parse(format!("invalid limit: {}", e))
                                        })?);
                                }
                                _ => {}
                            }
                        }
                    }
                    Rule::mutation_stmt => {
                        let stmt = body.into_inner().next().ok_or_else(|| {
                            NanoError::Parse("mutation statement cannot be empty".to_string())
                        })?;
                        mutation = Some(parse_mutation_stmt(stmt)?);
                    }
                    _ => {}
                }
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
        mutation,
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
    let value = parse_match_value(value_pair)?;

    Ok(PropMatch { prop_name, value })
}

fn parse_mutation_stmt(pair: pest::iterators::Pair<Rule>) -> Result<Mutation> {
    match pair.as_rule() {
        Rule::insert_stmt => parse_insert_mutation(pair).map(Mutation::Insert),
        Rule::update_stmt => parse_update_mutation(pair).map(Mutation::Update),
        Rule::delete_stmt => parse_delete_mutation(pair).map(Mutation::Delete),
        other => Err(NanoError::Parse(format!(
            "unexpected mutation statement rule: {:?}",
            other
        ))),
    }
}

fn parse_insert_mutation(pair: pest::iterators::Pair<Rule>) -> Result<InsertMutation> {
    let mut inner = pair.into_inner();
    let type_name = inner.next().unwrap().as_str().to_string();
    let mut assignments = Vec::new();
    for item in inner {
        if let Rule::mutation_assignment = item.as_rule() {
            assignments.push(parse_mutation_assignment(item)?);
        }
    }
    Ok(InsertMutation {
        type_name,
        assignments,
    })
}

fn parse_update_mutation(pair: pest::iterators::Pair<Rule>) -> Result<UpdateMutation> {
    let mut inner = pair.into_inner();
    let type_name = inner.next().unwrap().as_str().to_string();

    let mut assignments = Vec::new();
    let mut predicate = None;

    for item in inner {
        match item.as_rule() {
            Rule::mutation_assignment => assignments.push(parse_mutation_assignment(item)?),
            Rule::mutation_predicate => predicate = Some(parse_mutation_predicate(item)?),
            _ => {}
        }
    }

    let predicate = predicate.ok_or_else(|| {
        NanoError::Parse("update mutation requires a where predicate".to_string())
    })?;

    Ok(UpdateMutation {
        type_name,
        assignments,
        predicate,
    })
}

fn parse_delete_mutation(pair: pest::iterators::Pair<Rule>) -> Result<DeleteMutation> {
    let mut inner = pair.into_inner();
    let type_name = inner.next().unwrap().as_str().to_string();
    let predicate = inner
        .next()
        .ok_or_else(|| NanoError::Parse("delete mutation requires a where predicate".to_string()))
        .and_then(parse_mutation_predicate)?;
    Ok(DeleteMutation {
        type_name,
        predicate,
    })
}

fn parse_mutation_assignment(pair: pest::iterators::Pair<Rule>) -> Result<MutationAssignment> {
    let mut inner = pair.into_inner();
    let property = inner.next().unwrap().as_str().to_string();
    let value = parse_match_value(inner.next().unwrap())?;
    Ok(MutationAssignment { property, value })
}

fn parse_mutation_predicate(pair: pest::iterators::Pair<Rule>) -> Result<MutationPredicate> {
    let mut inner = pair.into_inner();
    let property = inner.next().unwrap().as_str().to_string();
    let op = parse_comp_op(inner.next().unwrap())?;
    let value = parse_match_value(inner.next().unwrap())?;
    Ok(MutationPredicate {
        property,
        op,
        value,
    })
}

fn parse_match_value(pair: pest::iterators::Pair<Rule>) -> Result<MatchValue> {
    let value_inner = pair.into_inner().next().unwrap();
    match value_inner.as_rule() {
        Rule::variable => {
            let v = value_inner.as_str();
            Ok(MatchValue::Variable(
                v.strip_prefix('$').unwrap_or(v).to_string(),
            ))
        }
        Rule::literal => Ok(MatchValue::Literal(parse_literal(value_inner)?)),
        _ => Err(NanoError::Parse(format!(
            "unexpected match value: {:?}",
            value_inner.as_rule()
        ))),
    }
}

fn parse_traversal(pair: pest::iterators::Pair<Rule>) -> Result<Traversal> {
    let mut inner = pair.into_inner();
    let src_var = inner.next().unwrap().as_str();
    let src = src_var.strip_prefix('$').unwrap_or(src_var).to_string();
    let edge_name = inner.next().unwrap().as_str().to_string();
    let mut min_hops = 1u32;
    let mut max_hops = Some(1u32);

    let next = inner.next().unwrap();
    let dst_pair = if let Rule::traversal_bounds = next.as_rule() {
        let (min, max) = parse_traversal_bounds(next)?;
        min_hops = min;
        max_hops = max;
        inner
            .next()
            .ok_or_else(|| NanoError::Parse("traversal missing destination variable".to_string()))?
    } else {
        next
    };

    let dst_var = dst_pair.as_str();
    let dst = dst_var.strip_prefix('$').unwrap_or(dst_var).to_string();

    Ok(Traversal {
        src,
        edge_name,
        dst,
        min_hops,
        max_hops,
    })
}

fn parse_traversal_bounds(pair: pest::iterators::Pair<Rule>) -> Result<(u32, Option<u32>)> {
    let mut inner = pair.into_inner();
    let min = inner
        .next()
        .ok_or_else(|| NanoError::Parse("traversal bound missing min hop".to_string()))?
        .as_str()
        .parse::<u32>()
        .map_err(|e| NanoError::Parse(format!("invalid traversal min bound: {}", e)))?;
    let max = inner
        .next()
        .map(|p| {
            p.as_str()
                .parse::<u32>()
                .map_err(|e| NanoError::Parse(format!("invalid traversal max bound: {}", e)))
        })
        .transpose()?;
    Ok((min, max))
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
            Ok(Expr::Variable(v.strip_prefix('$').unwrap_or(v).to_string()))
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
                other => return Err(NanoError::Parse(format!("unknown aggregate: {}", other))),
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
        Rule::string_lit => Ok(Literal::String(parse_string_lit(inner.as_str()))),
        Rule::integer => {
            let n: i64 = inner
                .as_str()
                .parse()
                .map_err(|e| NanoError::Parse(format!("invalid integer: {}", e)))?;
            Ok(Literal::Integer(n))
        }
        Rule::float_lit => {
            let f: f64 = inner
                .as_str()
                .parse()
                .map_err(|e| NanoError::Parse(format!("invalid float: {}", e)))?;
            Ok(Literal::Float(f))
        }
        Rule::bool_lit => {
            let b = inner.as_str() == "true";
            Ok(Literal::Bool(b))
        }
        Rule::date_lit => {
            let date_str = inner
                .into_inner()
                .next()
                .map(|s| parse_string_lit(s.as_str()))
                .ok_or_else(|| NanoError::Parse("date literal requires a string".to_string()))?;
            Ok(Literal::Date(date_str))
        }
        Rule::datetime_lit => {
            let dt_str = inner
                .into_inner()
                .next()
                .map(|s| parse_string_lit(s.as_str()))
                .ok_or_else(|| {
                    NanoError::Parse("datetime literal requires a string".to_string())
                })?;
            Ok(Literal::DateTime(dt_str))
        }
        Rule::list_lit => {
            let mut items = Vec::new();
            for item in inner.into_inner() {
                if item.as_rule() == Rule::literal {
                    items.push(parse_literal(item)?);
                }
            }
            Ok(Literal::List(items))
        }
        _ => Err(NanoError::Parse(format!(
            "unexpected literal: {:?}",
            inner.as_rule()
        ))),
    }
}

fn parse_string_lit(raw: &str) -> String {
    if raw.len() >= 2 && raw.starts_with('"') && raw.ends_with('"') {
        raw[1..raw.len() - 1].to_string()
    } else {
        raw.to_string()
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
                assert_eq!(t.min_hops, 1);
                assert_eq!(t.max_hops, Some(1));
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
                        assert_eq!(t.min_hops, 1);
                        assert_eq!(t.max_hops, Some(1));
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
                assert_eq!(t.min_hops, 1);
                assert_eq!(t.max_hops, Some(1));
            }
            _ => panic!("expected Traversal"),
        }
    }

    #[test]
    fn test_parse_bounded_traversal() {
        let input = r#"
query q() {
    match {
        $a: Person
        $a knows{1,3} $b
    }
    return { $b.name }
}
"#;
        let qf = parse_query(input).unwrap();
        let q = &qf.queries[0];
        match &q.match_clause[1] {
            Clause::Traversal(t) => {
                assert_eq!(t.min_hops, 1);
                assert_eq!(t.max_hops, Some(3));
            }
            _ => panic!("expected Traversal"),
        }
    }

    #[test]
    fn test_parse_unbounded_traversal() {
        let input = r#"
query q() {
    match {
        $a: Person
        $a knows{1,} $b
    }
    return { $b.name }
}
"#;
        let qf = parse_query(input).unwrap();
        let q = &qf.queries[0];
        match &q.match_clause[1] {
            Clause::Traversal(t) => {
                assert_eq!(t.min_hops, 1);
                assert_eq!(t.max_hops, None);
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

    #[test]
    fn test_parse_insert_mutation() {
        let input = r#"
query add_person($name: String, $age: I32) {
    insert Person {
        name: $name
        age: $age
    }
}
"#;
        let qf = parse_query(input).unwrap();
        let q = &qf.queries[0];
        match q.mutation.as_ref().expect("expected mutation") {
            Mutation::Insert(ins) => {
                assert_eq!(ins.type_name, "Person");
                assert_eq!(ins.assignments.len(), 2);
            }
            _ => panic!("expected Insert mutation"),
        }
    }

    #[test]
    fn test_parse_update_mutation() {
        let input = r#"
query set_age($name: String, $age: I32) {
    update Person set {
        age: $age
    } where name = $name
}
"#;
        let qf = parse_query(input).unwrap();
        let q = &qf.queries[0];
        match q.mutation.as_ref().expect("expected mutation") {
            Mutation::Update(upd) => {
                assert_eq!(upd.type_name, "Person");
                assert_eq!(upd.assignments.len(), 1);
                assert_eq!(upd.predicate.property, "name");
                assert_eq!(upd.predicate.op, CompOp::Eq);
            }
            _ => panic!("expected Update mutation"),
        }
    }

    #[test]
    fn test_parse_delete_mutation() {
        let input = r#"
query drop_person($name: String) {
    delete Person where name = $name
}
"#;
        let qf = parse_query(input).unwrap();
        let q = &qf.queries[0];
        match q.mutation.as_ref().expect("expected mutation") {
            Mutation::Delete(del) => {
                assert_eq!(del.type_name, "Person");
                assert_eq!(del.predicate.property, "name");
                assert_eq!(del.predicate.op, CompOp::Eq);
            }
            _ => panic!("expected Delete mutation"),
        }
    }

    #[test]
    fn test_parse_date_and_datetime_literals() {
        let input = r#"
query dated() {
    match {
        $e: Event
        $e.on = date("2026-02-14")
        $e.at >= datetime("2026-02-14T10:00:00Z")
    }
    return { $e.id }
}
"#;
        let qf = parse_query(input).unwrap();
        let q = &qf.queries[0];
        match &q.match_clause[1] {
            Clause::Filter(f) => match &f.right {
                Expr::Literal(Literal::Date(v)) => assert_eq!(v, "2026-02-14"),
                other => panic!("expected date literal, got {:?}", other),
            },
            _ => panic!("expected Filter"),
        }
        match &q.match_clause[2] {
            Clause::Filter(f) => match &f.right {
                Expr::Literal(Literal::DateTime(v)) => assert_eq!(v, "2026-02-14T10:00:00Z"),
                other => panic!("expected datetime literal, got {:?}", other),
            },
            _ => panic!("expected Filter"),
        }
    }

    #[test]
    fn test_parse_list_literal() {
        let input = r#"
query listy() {
    match { $p: Person { tags: ["rust", "db"] } }
    return { $p.tags }
}
"#;
        let qf = parse_query(input).unwrap();
        let q = &qf.queries[0];
        match &q.match_clause[0] {
            Clause::Binding(b) => match &b.prop_matches[0].value {
                MatchValue::Literal(Literal::List(items)) => {
                    assert_eq!(items.len(), 2);
                }
                other => panic!("expected list literal, got {:?}", other),
            },
            _ => panic!("expected Binding"),
        }
    }

    #[test]
    fn test_parse_error_diagnostic_has_span() {
        let input = r#"
query q() {
    match {
        $p: Person
    }
    return { $p.name
}
"#;
        let err = parse_query_diagnostic(input).unwrap_err();
        assert!(err.span.is_some());
    }
}
