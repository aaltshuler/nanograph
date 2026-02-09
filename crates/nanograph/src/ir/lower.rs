use std::collections::HashSet;

use crate::catalog::Catalog;
use crate::error::Result;
use crate::query::ast::*;
use crate::query::typecheck::TypeContext;
use crate::types::Direction;

use super::*;

pub fn lower_query(
    catalog: &Catalog,
    query: &QueryDecl,
    type_ctx: &TypeContext,
) -> Result<QueryIR> {
    let param_names: HashSet<String> = query.params.iter().map(|p| p.name.clone()).collect();

    let mut pipeline = Vec::new();
    let mut bound_vars = HashSet::new();

    lower_clauses(
        catalog,
        &query.match_clause,
        type_ctx,
        &mut pipeline,
        &mut bound_vars,
        &param_names,
    )?;

    let return_exprs: Vec<IRProjection> = query
        .return_clause
        .iter()
        .map(|p| IRProjection {
            expr: lower_expr(&p.expr, &param_names),
            alias: p.alias.clone(),
        })
        .collect();

    let order_by: Vec<IROrdering> = query
        .order_clause
        .iter()
        .map(|o| IROrdering {
            expr: lower_expr(&o.expr, &param_names),
            descending: o.descending,
        })
        .collect();

    Ok(QueryIR {
        name: query.name.clone(),
        params: query.params.clone(),
        pipeline,
        return_exprs,
        order_by,
        limit: query.limit,
    })
}

fn lower_clauses(
    catalog: &Catalog,
    clauses: &[Clause],
    type_ctx: &TypeContext,
    pipeline: &mut Vec<IROp>,
    bound_vars: &mut HashSet<String>,
    param_names: &HashSet<String>,
) -> Result<()> {
    // Separate clause types for ordering: bindings first, then traversals, then filters
    let mut bindings = Vec::new();
    let mut traversals = Vec::new();
    let mut filters = Vec::new();
    let mut negations = Vec::new();

    for clause in clauses {
        match clause {
            Clause::Binding(b) => bindings.push(b),
            Clause::Traversal(t) => traversals.push(t),
            Clause::Filter(f) => filters.push(f),
            Clause::Negation(inner) => negations.push(inner),
        }
    }

    // Lower bindings into NodeScan ops
    for binding in &bindings {
        // Collect inline filters from prop matches
        let mut scan_filters = Vec::new();
        for pm in &binding.prop_matches {
            match &pm.value {
                MatchValue::Literal(lit) => {
                    scan_filters.push(IRFilter {
                        left: IRExpr::PropAccess {
                            variable: binding.variable.clone(),
                            property: pm.prop_name.clone(),
                        },
                        op: CompOp::Eq,
                        right: IRExpr::Literal(lit.clone()),
                    });
                }
                MatchValue::Variable(v) => {
                    let right = if param_names.contains(v) {
                        IRExpr::Param(v.clone())
                    } else {
                        IRExpr::Variable(v.clone())
                    };
                    scan_filters.push(IRFilter {
                        left: IRExpr::PropAccess {
                            variable: binding.variable.clone(),
                            property: pm.prop_name.clone(),
                        },
                        op: CompOp::Eq,
                        right,
                    });
                }
            }
        }

        pipeline.push(IROp::NodeScan {
            variable: binding.variable.clone(),
            type_name: binding.type_name.clone(),
            filters: scan_filters,
        });
        bound_vars.insert(binding.variable.clone());
    }

    // Lower traversals into Expand ops
    // Handle "cycle closing" — if both src and dst are already bound, use a filter
    for traversal in &traversals {
        let edge = catalog.lookup_edge_by_name(&traversal.edge_name).unwrap();

        // Determine direction from type context
        let direction = type_ctx
            .traversals
            .iter()
            .find(|rt| {
                rt.src == traversal.src && rt.dst == traversal.dst && rt.edge_type == edge.name
            })
            .map(|rt| rt.direction)
            .unwrap_or(Direction::Out);

        let dst_type = match direction {
            Direction::Out => edge.to_type.clone(),
            Direction::In => edge.from_type.clone(),
        };

        if bound_vars.contains(&traversal.src) && bound_vars.contains(&traversal.dst) {
            // Cycle closing: emit expand to a temp var, then filter temp.id = dst.id
            let temp_var = format!("__temp_{}", traversal.dst);
            pipeline.push(IROp::Expand {
                src_var: traversal.src.clone(),
                dst_var: temp_var.clone(),
                edge_type: edge.name.clone(),
                direction,
                dst_type,
            });
            pipeline.push(IROp::Filter(IRFilter {
                left: IRExpr::PropAccess {
                    variable: temp_var,
                    property: "id".to_string(),
                },
                op: CompOp::Eq,
                right: IRExpr::PropAccess {
                    variable: traversal.dst.clone(),
                    property: "id".to_string(),
                },
            }));
        } else if !bound_vars.contains(&traversal.src) && bound_vars.contains(&traversal.dst) {
            // Reverse expand: dst is bound, src is not.
            // Swap direction and expand from dst to discover src.
            let reverse_dir = match direction {
                Direction::Out => Direction::In,
                Direction::In => Direction::Out,
            };
            let src_type = match direction {
                Direction::Out => edge.from_type.clone(),
                Direction::In => edge.to_type.clone(),
            };
            pipeline.push(IROp::Expand {
                src_var: traversal.dst.clone(),
                dst_var: traversal.src.clone(),
                edge_type: edge.name.clone(),
                direction: reverse_dir,
                dst_type: src_type,
            });
            if traversal.src != "_" {
                bound_vars.insert(traversal.src.clone());
            }
        } else {
            pipeline.push(IROp::Expand {
                src_var: traversal.src.clone(),
                dst_var: traversal.dst.clone(),
                edge_type: edge.name.clone(),
                direction,
                dst_type,
            });
            if traversal.dst != "_" {
                bound_vars.insert(traversal.dst.clone());
            }
        }
    }

    // Lower explicit filters
    for filter in &filters {
        pipeline.push(IROp::Filter(IRFilter {
            left: lower_expr(&filter.left, param_names),
            op: filter.op,
            right: lower_expr(&filter.right, param_names),
        }));
    }

    // Lower negations into AntiJoin ops
    for neg_clauses in &negations {
        // Find outer-bound variable referenced in the negation
        let outer_var = find_outer_var(neg_clauses, bound_vars);

        let mut inner_pipeline = Vec::new();
        let mut inner_bound = bound_vars.clone();
        lower_clauses(
            catalog,
            neg_clauses,
            type_ctx,
            &mut inner_pipeline,
            &mut inner_bound,
            param_names,
        )?;

        pipeline.push(IROp::AntiJoin {
            outer_var: outer_var.unwrap_or_default(),
            inner: inner_pipeline,
        });
    }

    Ok(())
}

fn find_outer_var(clauses: &[Clause], outer_bound: &HashSet<String>) -> Option<String> {
    for clause in clauses {
        match clause {
            Clause::Traversal(t) => {
                if outer_bound.contains(&t.src) {
                    return Some(t.src.clone());
                }
                if outer_bound.contains(&t.dst) {
                    return Some(t.dst.clone());
                }
            }
            Clause::Filter(f) => {
                if let Some(v) = expr_var(&f.left) {
                    if outer_bound.contains(&v) {
                        return Some(v);
                    }
                }
                if let Some(v) = expr_var(&f.right) {
                    if outer_bound.contains(&v) {
                        return Some(v);
                    }
                }
            }
            Clause::Binding(b) => {
                if outer_bound.contains(&b.variable) {
                    return Some(b.variable.clone());
                }
            }
            _ => {}
        }
    }
    None
}

fn expr_var(expr: &Expr) -> Option<String> {
    match expr {
        Expr::PropAccess { variable, .. } => Some(variable.clone()),
        Expr::Variable(v) => Some(v.clone()),
        _ => None,
    }
}

fn lower_expr(expr: &Expr, param_names: &HashSet<String>) -> IRExpr {
    match expr {
        Expr::PropAccess { variable, property } => IRExpr::PropAccess {
            variable: variable.clone(),
            property: property.clone(),
        },
        Expr::Variable(v) => {
            if param_names.contains(v) {
                IRExpr::Param(v.clone())
            } else {
                IRExpr::Variable(v.clone())
            }
        }
        Expr::Literal(l) => IRExpr::Literal(l.clone()),
        Expr::Aggregate { func, arg } => IRExpr::Aggregate {
            func: *func,
            arg: Box::new(lower_expr(arg, param_names)),
        },
        Expr::AliasRef(name) => IRExpr::AliasRef(name.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::build_catalog;
    use crate::query::parser::parse_query;
    use crate::query::typecheck::typecheck_query;
    use crate::schema::parser::parse_schema;

    fn setup() -> Catalog {
        let schema = parse_schema(
            r#"
node Person { name: String  age: I32? }
node Company { name: String }
edge Knows: Person -> Person { since: Date? }
edge WorksAt: Person -> Company
"#,
        )
        .unwrap();
        build_catalog(&schema).unwrap()
    }

    #[test]
    fn test_lower_basic() {
        let catalog = setup();
        let qf = parse_query(
            r#"
query q($name: String) {
    match {
        $p: Person { name: $name }
        $p knows $f
    }
    return { $f.name, $f.age }
}
"#,
        )
        .unwrap();
        let tc = typecheck_query(&catalog, &qf.queries[0]).unwrap();
        let ir = lower_query(&catalog, &qf.queries[0], &tc).unwrap();

        assert_eq!(ir.pipeline.len(), 2); // NodeScan + Expand
        assert_eq!(ir.return_exprs.len(), 2);
    }

    #[test]
    fn test_lower_negation() {
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
        let tc = typecheck_query(&catalog, &qf.queries[0]).unwrap();
        let ir = lower_query(&catalog, &qf.queries[0], &tc).unwrap();

        assert_eq!(ir.pipeline.len(), 2); // NodeScan + AntiJoin
        matches!(&ir.pipeline[1], IROp::AntiJoin { .. });
    }
}
