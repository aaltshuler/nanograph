use std::collections::HashMap;

use crate::catalog::Catalog;
use crate::error::{NanoError, Result};
use crate::types::{Direction, ScalarType};

use super::ast::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BindingKind {
    Node,
    Edge,
}

#[derive(Debug, Clone)]
pub struct BoundVariable {
    pub var_name: String,
    pub type_name: String,
    pub kind: BindingKind,
}

#[derive(Debug, Clone)]
pub struct TypeContext {
    pub bindings: HashMap<String, BoundVariable>,
    pub aliases: HashMap<String, ResolvedType>,
    pub traversals: Vec<ResolvedTraversal>,
}

#[derive(Debug, Clone)]
pub struct ResolvedTraversal {
    pub src: String,
    pub dst: String,
    pub edge_type: String,
    pub direction: Direction,
    pub min_hops: u32,
    pub max_hops: Option<u32>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ResolvedType {
    Scalar(ScalarType, bool),
    Node(String),
    Aggregate,
}

#[derive(Debug, Clone)]
pub struct MutationTypeContext {
    pub target_type: String,
}

#[derive(Debug, Clone)]
pub enum CheckedQuery {
    Read(TypeContext),
    Mutation(MutationTypeContext),
}

pub fn typecheck_query_decl(catalog: &Catalog, query: &QueryDecl) -> Result<CheckedQuery> {
    if let Some(mutation) = &query.mutation {
        let target_type = typecheck_mutation(catalog, mutation, &query.params)?;
        Ok(CheckedQuery::Mutation(MutationTypeContext { target_type }))
    } else {
        Ok(CheckedQuery::Read(typecheck_read_query(catalog, query)?))
    }
}

pub fn typecheck_query(catalog: &Catalog, query: &QueryDecl) -> Result<TypeContext> {
    if query.mutation.is_some() {
        return Err(NanoError::Type(
            "mutation query cannot be typechecked with read-query API".to_string(),
        ));
    }
    typecheck_read_query(catalog, query)
}

fn typecheck_read_query(catalog: &Catalog, query: &QueryDecl) -> Result<TypeContext> {
    let mut ctx = TypeContext {
        bindings: HashMap::new(),
        aliases: HashMap::new(),
        traversals: Vec::new(),
    };

    let params: HashMap<String, ScalarType> = query
        .params
        .iter()
        .filter_map(|p| ScalarType::from_str_name(&p.type_name).map(|s| (p.name.clone(), s)))
        .collect();

    // Typecheck match clauses
    typecheck_clauses(catalog, &query.match_clause, &mut ctx, &params, false)?;

    // Typecheck return projections
    for proj in &query.return_clause {
        let resolved = resolve_expr_type(catalog, &proj.expr, &ctx, &params)?;
        if let Some(alias) = &proj.alias {
            ctx.aliases.insert(alias.clone(), resolved);
        }
    }

    // Typecheck order expressions
    for ord in &query.order_clause {
        resolve_expr_type(catalog, &ord.expr, &ctx, &params)?;
    }

    Ok(ctx)
}

fn typecheck_mutation(catalog: &Catalog, mutation: &Mutation, params: &[Param]) -> Result<String> {
    let param_types: HashMap<String, ScalarType> = params
        .iter()
        .filter_map(|p| ScalarType::from_str_name(&p.type_name).map(|s| (p.name.clone(), s)))
        .collect();

    match mutation {
        Mutation::Insert(insert) => {
            let node_type = catalog.node_types.get(&insert.type_name).ok_or_else(|| {
                NanoError::Type(format!("T10: unknown node type `{}`", insert.type_name))
            })?;

            if insert.assignments.is_empty() {
                return Err(NanoError::Type(
                    "T10: insert mutation requires at least one assignment".to_string(),
                ));
            }

            ensure_no_duplicate_assignment_names(&insert.assignments)?;

            for assignment in &insert.assignments {
                let prop_type =
                    node_type
                        .properties
                        .get(&assignment.property)
                        .ok_or_else(|| {
                            NanoError::Type(format!(
                                "T11: type `{}` has no property `{}`",
                                insert.type_name, assignment.property
                            ))
                        })?;
                check_match_value_type(
                    &assignment.value,
                    &param_types,
                    &prop_type.scalar,
                    &assignment.property,
                )?;
            }

            for (prop_name, prop_type) in &node_type.properties {
                if prop_type.nullable {
                    continue;
                }
                if !insert.assignments.iter().any(|a| &a.property == prop_name) {
                    return Err(NanoError::Type(format!(
                        "T12: insert for `{}` must provide non-nullable property `{}`",
                        insert.type_name, prop_name
                    )));
                }
            }

            Ok(insert.type_name.clone())
        }
        Mutation::Update(update) => {
            let node_type = catalog.node_types.get(&update.type_name).ok_or_else(|| {
                NanoError::Type(format!("T10: unknown node type `{}`", update.type_name))
            })?;

            if update.assignments.is_empty() {
                return Err(NanoError::Type(
                    "T10: update mutation requires at least one assignment".to_string(),
                ));
            }
            ensure_no_duplicate_assignment_names(&update.assignments)?;

            for assignment in &update.assignments {
                let prop_type =
                    node_type
                        .properties
                        .get(&assignment.property)
                        .ok_or_else(|| {
                            NanoError::Type(format!(
                                "T11: type `{}` has no property `{}`",
                                update.type_name, assignment.property
                            ))
                        })?;
                check_match_value_type(
                    &assignment.value,
                    &param_types,
                    &prop_type.scalar,
                    &assignment.property,
                )?;
            }

            typecheck_mutation_predicate(
                &update.type_name,
                &update.predicate,
                node_type,
                &param_types,
            )?;
            Ok(update.type_name.clone())
        }
        Mutation::Delete(delete) => {
            let node_type = catalog.node_types.get(&delete.type_name).ok_or_else(|| {
                NanoError::Type(format!("T10: unknown node type `{}`", delete.type_name))
            })?;
            typecheck_mutation_predicate(
                &delete.type_name,
                &delete.predicate,
                node_type,
                &param_types,
            )?;
            Ok(delete.type_name.clone())
        }
    }
}

fn ensure_no_duplicate_assignment_names(assignments: &[MutationAssignment]) -> Result<()> {
    let mut seen = std::collections::HashSet::new();
    for assignment in assignments {
        if !seen.insert(&assignment.property) {
            return Err(NanoError::Type(format!(
                "T13: duplicate assignment for property `{}`",
                assignment.property
            )));
        }
    }
    Ok(())
}

fn typecheck_mutation_predicate(
    type_name: &str,
    predicate: &MutationPredicate,
    node_type: &crate::catalog::NodeType,
    param_types: &HashMap<String, ScalarType>,
) -> Result<()> {
    let prop_type = node_type
        .properties
        .get(&predicate.property)
        .ok_or_else(|| {
            NanoError::Type(format!(
                "T11: type `{}` has no property `{}`",
                type_name, predicate.property
            ))
        })?;
    check_match_value_type(
        &predicate.value,
        param_types,
        &prop_type.scalar,
        &predicate.property,
    )?;
    Ok(())
}

fn check_match_value_type(
    value: &MatchValue,
    params: &HashMap<String, ScalarType>,
    expected: &ScalarType,
    property: &str,
) -> Result<()> {
    match value {
        MatchValue::Literal(lit) => check_literal_type(lit, expected, property),
        MatchValue::Variable(v) => {
            let Some(actual) = params.get(v) else {
                return Err(NanoError::Type(format!(
                    "T14: mutation variable `${}` must be a declared query parameter",
                    v
                )));
            };
            if !types_compatible(actual, expected) {
                return Err(NanoError::Type(format!(
                    "T7: cannot assign/compare {} with {} for property `{}`",
                    actual, expected, property
                )));
            }
            Ok(())
        }
    }
}

fn typecheck_clauses(
    catalog: &Catalog,
    clauses: &[Clause],
    ctx: &mut TypeContext,
    params: &HashMap<String, ScalarType>,
    _in_negation: bool,
) -> Result<()> {
    for clause in clauses {
        match clause {
            Clause::Binding(b) => typecheck_binding(catalog, b, ctx)?,
            Clause::Traversal(t) => typecheck_traversal(catalog, t, ctx)?,
            Clause::Filter(f) => typecheck_filter(catalog, f, ctx, params)?,
            Clause::Negation(inner) => {
                // T9: at least one variable in the negation block must be bound outside
                let outer_vars: Vec<String> = ctx.bindings.keys().cloned().collect();

                // Typecheck inner clauses in a copy of ctx
                let mut inner_ctx = ctx.clone();
                typecheck_clauses(catalog, inner, &mut inner_ctx, params, true)?;

                // Check T9
                let mut has_outer = false;
                for clause in inner {
                    match clause {
                        Clause::Traversal(t) => {
                            if outer_vars.contains(&t.src) || outer_vars.contains(&t.dst) {
                                has_outer = true;
                            }
                        }
                        Clause::Filter(f) => {
                            if expr_references_any(&f.left, &outer_vars)
                                || expr_references_any(&f.right, &outer_vars)
                            {
                                has_outer = true;
                            }
                        }
                        Clause::Binding(b) => {
                            if outer_vars.contains(&b.variable) {
                                has_outer = true;
                            }
                        }
                        _ => {}
                    }
                }
                if !has_outer {
                    return Err(NanoError::Type(
                        "T9: negation block must reference at least one outer-bound variable"
                            .to_string(),
                    ));
                }
            }
        }
    }
    Ok(())
}

fn typecheck_binding(catalog: &Catalog, binding: &Binding, ctx: &mut TypeContext) -> Result<()> {
    // T1: binding type must exist in catalog
    if !catalog.node_types.contains_key(&binding.type_name) {
        return Err(NanoError::Type(format!(
            "T1: unknown node type `{}`",
            binding.type_name
        )));
    }

    let node_type = &catalog.node_types[&binding.type_name];

    // T2 + T3: property match fields must exist and have correct types
    for pm in &binding.prop_matches {
        let prop = node_type.properties.get(&pm.prop_name).ok_or_else(|| {
            NanoError::Type(format!(
                "T2: type `{}` has no property `{}`",
                binding.type_name, pm.prop_name
            ))
        })?;

        // T3: check value type matches property type
        match &pm.value {
            MatchValue::Literal(lit) => {
                check_literal_type(lit, &prop.scalar, &pm.prop_name)?;
            }
            MatchValue::Variable(_) => {
                // Variable match — will be unified, no static check needed here
            }
        }
    }

    // Don't overwrite if already bound to same type (re-binding same var is OK)
    if let Some(existing) = ctx.bindings.get(&binding.variable) {
        if existing.type_name != binding.type_name {
            return Err(NanoError::Type(format!(
                "variable `${}` already bound to type `{}`, cannot rebind to `{}`",
                binding.variable, existing.type_name, binding.type_name
            )));
        }
    }

    ctx.bindings.insert(
        binding.variable.clone(),
        BoundVariable {
            var_name: binding.variable.clone(),
            type_name: binding.type_name.clone(),
            kind: BindingKind::Node,
        },
    );

    Ok(())
}

fn typecheck_traversal(
    catalog: &Catalog,
    traversal: &Traversal,
    ctx: &mut TypeContext,
) -> Result<()> {
    // T4: edge must exist
    let edge = catalog
        .lookup_edge_by_name(&traversal.edge_name)
        .ok_or_else(|| {
            NanoError::Type(format!("T4: unknown edge type `{}`", traversal.edge_name))
        })?;

    if traversal.min_hops == 0 {
        return Err(NanoError::Type(
            "T15: traversal min hop bound must be >= 1".to_string(),
        ));
    }
    if let Some(max_hops) = traversal.max_hops {
        if max_hops < traversal.min_hops {
            return Err(NanoError::Type(format!(
                "T15: invalid traversal bounds {{{},{}}}; max must be >= min",
                traversal.min_hops, max_hops
            )));
        }
    } else {
        return Err(NanoError::Type(
            "T15: unbounded traversal is disabled; use bounded traversal {min,max}".to_string(),
        ));
    }

    // Determine direction based on bound variables and edge endpoints
    let src_bound = ctx.bindings.get(&traversal.src);
    let dst_bound = ctx.bindings.get(&traversal.dst);

    let direction;

    if let Some(src_bv) = src_bound {
        // T5: src type must match one endpoint of the edge
        if src_bv.type_name == edge.from_type {
            direction = Direction::Out;
            // dst should be edge.to_type
            bind_traversal_endpoint(ctx, &traversal.dst, &edge.to_type, edge)?;
        } else if src_bv.type_name == edge.to_type {
            direction = Direction::In;
            // dst should be edge.from_type
            bind_traversal_endpoint(ctx, &traversal.dst, &edge.from_type, edge)?;
        } else {
            return Err(NanoError::Type(format!(
                "T5: variable `${}` has type `{}`, which is not an endpoint of edge `{}: {} -> {}`",
                traversal.src, src_bv.type_name, edge.name, edge.from_type, edge.to_type
            )));
        }
    } else if let Some(dst_bv) = dst_bound {
        // dst is bound, infer direction from it
        if dst_bv.type_name == edge.to_type {
            direction = Direction::Out;
            bind_traversal_endpoint(ctx, &traversal.src, &edge.from_type, edge)?;
        } else if dst_bv.type_name == edge.from_type {
            direction = Direction::In;
            bind_traversal_endpoint(ctx, &traversal.src, &edge.to_type, edge)?;
        } else {
            return Err(NanoError::Type(format!(
                "T5: variable `${}` has type `{}`, which is not an endpoint of edge `{}: {} -> {}`",
                traversal.dst, dst_bv.type_name, edge.name, edge.from_type, edge.to_type
            )));
        }
    } else {
        // Neither bound — default Out direction, bind both
        direction = Direction::Out;
        bind_traversal_endpoint(ctx, &traversal.src, &edge.from_type, edge)?;
        bind_traversal_endpoint(ctx, &traversal.dst, &edge.to_type, edge)?;
    }

    ctx.traversals.push(ResolvedTraversal {
        src: traversal.src.clone(),
        dst: traversal.dst.clone(),
        edge_type: edge.name.clone(),
        direction,
        min_hops: traversal.min_hops,
        max_hops: traversal.max_hops,
    });

    Ok(())
}

fn bind_traversal_endpoint(
    ctx: &mut TypeContext,
    var: &str,
    expected_type: &str,
    edge: &crate::catalog::EdgeType,
) -> Result<()> {
    if var == "_" {
        return Ok(()); // anonymous variable
    }
    if let Some(existing) = ctx.bindings.get(var) {
        if existing.type_name != expected_type {
            return Err(NanoError::Type(format!(
                "T5: variable `${}` has type `{}` but edge `{}` expects `{}`",
                var, existing.type_name, edge.name, expected_type
            )));
        }
    } else {
        ctx.bindings.insert(
            var.to_string(),
            BoundVariable {
                var_name: var.to_string(),
                type_name: expected_type.to_string(),
                kind: BindingKind::Node,
            },
        );
    }
    Ok(())
}

fn typecheck_filter(
    catalog: &Catalog,
    filter: &Filter,
    ctx: &TypeContext,
    params: &HashMap<String, ScalarType>,
) -> Result<()> {
    let left_type = resolve_expr_type(catalog, &filter.left, ctx, params)?;
    let right_type = resolve_expr_type(catalog, &filter.right, ctx, params)?;

    // T7: check type compatibility
    match (&left_type, &right_type) {
        (ResolvedType::Scalar(l, _), ResolvedType::Scalar(r, _)) => {
            if !types_compatible(l, r) {
                return Err(NanoError::Type(format!(
                    "T7: cannot compare {} with {}",
                    l, r
                )));
            }
        }
        _ => {} // node or aggregate types — comparison may be on ids etc
    }

    Ok(())
}

fn resolve_expr_type(
    catalog: &Catalog,
    expr: &Expr,
    ctx: &TypeContext,
    params: &HashMap<String, ScalarType>,
) -> Result<ResolvedType> {
    match expr {
        Expr::PropAccess { variable, property } => {
            // T6: variable must be bound and property must exist
            let bv = ctx.bindings.get(variable).ok_or_else(|| {
                NanoError::Type(format!("T6: variable `${}` is not bound", variable))
            })?;

            let node_type = catalog.node_types.get(&bv.type_name).ok_or_else(|| {
                NanoError::Type(format!("T6: type `{}` not found in catalog", bv.type_name))
            })?;

            let prop = node_type.properties.get(property).ok_or_else(|| {
                NanoError::Type(format!(
                    "T6: type `{}` has no property `{}`",
                    bv.type_name, property
                ))
            })?;

            Ok(ResolvedType::Scalar(prop.scalar, prop.nullable))
        }
        Expr::Variable(name) => {
            // Could be a query parameter or a bound variable
            if let Some(scalar) = params.get(name) {
                Ok(ResolvedType::Scalar(*scalar, false))
            } else if let Some(bv) = ctx.bindings.get(name) {
                Ok(ResolvedType::Node(bv.type_name.clone()))
            } else {
                Err(NanoError::Type(format!(
                    "variable `${}` is not bound",
                    name
                )))
            }
        }
        Expr::Literal(lit) => Ok(ResolvedType::Scalar(literal_type(lit), false)),
        Expr::Aggregate { func, arg } => {
            let arg_type = resolve_expr_type(catalog, arg, ctx, params)?;

            // T8: sum/avg/min/max require numeric
            match func {
                AggFunc::Sum | AggFunc::Avg | AggFunc::Min | AggFunc::Max => {
                    if let ResolvedType::Scalar(s, _) = &arg_type {
                        if !s.is_numeric() {
                            return Err(NanoError::Type(format!(
                                "T8: {} requires numeric type, got {}",
                                func, s
                            )));
                        }
                    }
                }
                _ => {} // count works on any type
            }

            Ok(ResolvedType::Aggregate)
        }
        Expr::AliasRef(name) => {
            // Check if it's a known alias from return clause
            if let Some(resolved) = ctx.aliases.get(name) {
                Ok(resolved.clone())
            } else {
                // Might be an alias not yet registered (forward reference in order)
                Ok(ResolvedType::Aggregate)
            }
        }
    }
}

fn literal_type(lit: &Literal) -> ScalarType {
    match lit {
        Literal::String(_) => ScalarType::String,
        Literal::Integer(_) => ScalarType::I64,
        Literal::Float(_) => ScalarType::F64,
        Literal::Bool(_) => ScalarType::Bool,
    }
}

fn check_literal_type(lit: &Literal, expected: &ScalarType, prop_name: &str) -> Result<()> {
    let lit_type = literal_type(lit);
    if !types_compatible(&lit_type, expected) {
        return Err(NanoError::Type(format!(
            "T3: property `{}` has type {} but got {}",
            prop_name, expected, lit_type
        )));
    }
    Ok(())
}

fn types_compatible(a: &ScalarType, b: &ScalarType) -> bool {
    if a == b {
        return true;
    }
    // Numeric types are mutually compatible for comparison
    if a.is_numeric() && b.is_numeric() {
        return true;
    }
    false
}

fn expr_references_any(expr: &Expr, vars: &[String]) -> bool {
    match expr {
        Expr::PropAccess { variable, .. } => vars.contains(variable),
        Expr::Variable(v) => vars.contains(v),
        Expr::Aggregate { arg, .. } => expr_references_any(arg, vars),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::build_catalog;
    use crate::query::parser::parse_query;
    use crate::schema::parser::parse_schema;

    fn setup() -> Catalog {
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
edge WorksAt: Person -> Company {
    title: String?
}
"#,
        )
        .unwrap();
        build_catalog(&schema).unwrap()
    }

    #[test]
    fn test_basic_binding() {
        let catalog = setup();
        let qf = parse_query(
            r#"
query q() {
    match { $p: Person }
    return { $p.name }
}
"#,
        )
        .unwrap();
        let ctx = typecheck_query(&catalog, &qf.queries[0]).unwrap();
        assert!(ctx.bindings.contains_key("p"));
    }

    #[test]
    fn test_t1_unknown_type() {
        let catalog = setup();
        let qf = parse_query(
            r#"
query q() {
    match { $p: Foo }
    return { $p.name }
}
"#,
        )
        .unwrap();
        let err = typecheck_query(&catalog, &qf.queries[0]).unwrap_err();
        assert!(err.to_string().contains("T1"));
    }

    #[test]
    fn test_t2_unknown_property_match() {
        let catalog = setup();
        let qf = parse_query(
            r#"
query q() {
    match { $p: Person { salary: 100 } }
    return { $p.name }
}
"#,
        )
        .unwrap();
        let err = typecheck_query(&catalog, &qf.queries[0]).unwrap_err();
        assert!(err.to_string().contains("T2"));
    }

    #[test]
    fn test_t3_wrong_type_in_match() {
        let catalog = setup();
        let qf = parse_query(
            r#"
query q() {
    match { $p: Person { age: "old" } }
    return { $p.name }
}
"#,
        )
        .unwrap();
        let err = typecheck_query(&catalog, &qf.queries[0]).unwrap_err();
        assert!(err.to_string().contains("T3"));
    }

    #[test]
    fn test_t4_unknown_edge() {
        let catalog = setup();
        let qf = parse_query(
            r#"
query q() {
    match {
        $p: Person
        $p likes $f
    }
    return { $p.name }
}
"#,
        )
        .unwrap();
        let err = typecheck_query(&catalog, &qf.queries[0]).unwrap_err();
        assert!(err.to_string().contains("T4"));
    }

    #[test]
    fn test_t5_bad_endpoints() {
        let catalog = setup();
        let qf = parse_query(
            r#"
query q() {
    match {
        $c: Company
        $c knows $f
    }
    return { $c.name }
}
"#,
        )
        .unwrap();
        let err = typecheck_query(&catalog, &qf.queries[0]).unwrap_err();
        assert!(err.to_string().contains("T5"));
    }

    #[test]
    fn test_t6_bad_property() {
        let catalog = setup();
        let qf = parse_query(
            r#"
query q() {
    match {
        $p: Person
        $p.salary > 100
    }
    return { $p.name }
}
"#,
        )
        .unwrap();
        let err = typecheck_query(&catalog, &qf.queries[0]).unwrap_err();
        assert!(err.to_string().contains("T6"));
    }

    #[test]
    fn test_t7_bad_comparison() {
        let catalog = setup();
        let qf = parse_query(
            r#"
query q() {
    match {
        $p: Person
        $p.age > "old"
    }
    return { $p.name }
}
"#,
        )
        .unwrap();
        let err = typecheck_query(&catalog, &qf.queries[0]).unwrap_err();
        assert!(err.to_string().contains("T7"));
    }

    #[test]
    fn test_t8_sum_on_string() {
        let catalog = setup();
        let qf = parse_query(
            r#"
query q() {
    match { $p: Person }
    return { sum($p.name) as s }
}
"#,
        )
        .unwrap();
        let err = typecheck_query(&catalog, &qf.queries[0]).unwrap_err();
        assert!(err.to_string().contains("T8"));
    }

    #[test]
    fn test_traversal_direction_out() {
        let catalog = setup();
        let qf = parse_query(
            r#"
query q() {
    match {
        $p: Person { name: "Alice" }
        $p knows $f
    }
    return { $f.name }
}
"#,
        )
        .unwrap();
        let ctx = typecheck_query(&catalog, &qf.queries[0]).unwrap();
        assert_eq!(ctx.traversals[0].direction, Direction::Out);
        assert_eq!(ctx.bindings["f"].type_name, "Person");
    }

    #[test]
    fn test_traversal_direction_in() {
        let catalog = setup();
        let qf = parse_query(
            r#"
query q() {
    match {
        $c: Company { name: "Acme" }
        $p worksAt $c
    }
    return { $p.name }
}
"#,
        )
        .unwrap();
        let ctx = typecheck_query(&catalog, &qf.queries[0]).unwrap();
        // $c is Company (to_type), $p is src — direction should be Out
        // because $p (Person=from_type) worksAt $c (Company=to_type) is forward
        assert_eq!(ctx.traversals[0].direction, Direction::Out);
    }

    #[test]
    fn test_bounded_traversal_typecheck() {
        let catalog = setup();
        let qf = parse_query(
            r#"
query q() {
    match {
        $p: Person
        $p knows{1,3} $f
    }
    return { $f.name }
}
"#,
        )
        .unwrap();
        let ctx = typecheck_query(&catalog, &qf.queries[0]).unwrap();
        assert_eq!(ctx.traversals[0].min_hops, 1);
        assert_eq!(ctx.traversals[0].max_hops, Some(3));
    }

    #[test]
    fn test_bounded_traversal_invalid_bounds() {
        let catalog = setup();
        let qf = parse_query(
            r#"
query q() {
    match {
        $p: Person
        $p knows{3,1} $f
    }
    return { $f.name }
}
"#,
        )
        .unwrap();
        let err = typecheck_query(&catalog, &qf.queries[0]).unwrap_err();
        assert!(err.to_string().contains("T15"));
    }

    #[test]
    fn test_unbounded_traversal_is_disabled() {
        let catalog = setup();
        let qf = parse_query(
            r#"
query q() {
    match {
        $p: Person
        $p knows{1,} $f
    }
    return { $f.name }
}
"#,
        )
        .unwrap();
        let err = typecheck_query(&catalog, &qf.queries[0]).unwrap_err();
        assert!(err.to_string().contains("unbounded traversal is disabled"));
    }

    #[test]
    fn test_negation_typecheck() {
        let catalog = setup();
        let qf = parse_query(
            r#"
query q() {
    match {
        $p: Person
        not { $p worksAt $_ }
    }
    return { $p.name }
}
"#,
        )
        .unwrap();
        let ctx = typecheck_query(&catalog, &qf.queries[0]).unwrap();
        assert!(ctx.bindings.contains_key("p"));
    }

    #[test]
    fn test_aggregation_typecheck() {
        let catalog = setup();
        let qf = parse_query(
            r#"
query q() {
    match {
        $p: Person
        $p knows $f
    }
    return {
        $p.name
        count($f) as friends
    }
}
"#,
        )
        .unwrap();
        typecheck_query(&catalog, &qf.queries[0]).unwrap();
    }

    #[test]
    fn test_valid_two_hop() {
        let catalog = setup();
        let qf = parse_query(
            r#"
query q($name: String) {
    match {
        $p: Person { name: $name }
        $p knows $mid
        $mid knows $fof
    }
    return { $fof.name }
}
"#,
        )
        .unwrap();
        let ctx = typecheck_query(&catalog, &qf.queries[0]).unwrap();
        assert!(ctx.bindings.contains_key("mid"));
        assert!(ctx.bindings.contains_key("fof"));
    }

    #[test]
    fn test_mutation_insert_typecheck_ok() {
        let catalog = setup();
        let qf = parse_query(
            r#"
query add_person($name: String, $age: I32) {
    insert Person {
        name: $name
        age: $age
    }
}
"#,
        )
        .unwrap();
        let checked = typecheck_query_decl(&catalog, &qf.queries[0]).unwrap();
        match checked {
            CheckedQuery::Mutation(ctx) => assert_eq!(ctx.target_type, "Person"),
            _ => panic!("expected mutation typecheck result"),
        }
    }

    #[test]
    fn test_mutation_insert_missing_required_property() {
        let catalog = setup();
        let qf = parse_query(
            r#"
query add_person($age: I32) {
    insert Person { age: $age }
}
"#,
        )
        .unwrap();
        let err = typecheck_query_decl(&catalog, &qf.queries[0]).unwrap_err();
        assert!(err.to_string().contains("T12"));
    }

    #[test]
    fn test_mutation_update_bad_property() {
        let catalog = setup();
        let qf = parse_query(
            r#"
query update_person($name: String) {
    update Person set { salary: 100 } where name = $name
}
"#,
        )
        .unwrap();
        let err = typecheck_query_decl(&catalog, &qf.queries[0]).unwrap_err();
        assert!(err.to_string().contains("T11"));
    }

    #[test]
    fn test_mutation_delete_bad_type() {
        let catalog = setup();
        let qf = parse_query(
            r#"
query del($name: String) {
    delete Unknown where name = $name
}
"#,
        )
        .unwrap();
        let err = typecheck_query_decl(&catalog, &qf.queries[0]).unwrap_err();
        assert!(err.to_string().contains("T10"));
    }
}
