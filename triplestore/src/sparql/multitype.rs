use oxrdf::vocab::xsd;
use oxrdf::{BlankNode, Literal, NamedNode};
use polars::prelude::{coalesce, col, IntoLazy, LazyFrame};
use polars_core::frame::DataFrame;
use polars_core::prelude::{
    AnyValue, ChunkedArray, NamedFrom, NewChunkedArray, ObjectChunked, PolarsObject,
};
use polars_core::series::Series;
use representation::RDFNodeType;
use spargebra::algebra::{Expression, OrderExpression};
use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};

pub const MULTI_TYPE_NAME: &str = "MultiTypes";

#[derive(Debug, Clone, Default, Eq, Hash, PartialEq)]
pub enum MultiType {
    IRI(NamedNode),
    BlankNode(BlankNode),
    Literal(Literal),
    #[default]
    Null,
}

impl Display for MultiType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            MultiType::IRI(i) => {
                write!(f, "{}", i)
            }
            MultiType::BlankNode(b) => {
                write!(f, "{}", b)
            }
            MultiType::Literal(l) => {
                write!(f, "{}", l)
            }
            MultiType::Null => {
                write!(f, "Null")
            }
        }
    }
}

impl PolarsObject for MultiType {
    fn type_name() -> &'static str {
        MULTI_TYPE_NAME
    }
}

pub fn convert_df_col_to_multitype(df: &mut DataFrame, col: &str, dt: &RDFNodeType) {
    let ser = df.column(col).unwrap();
    let new_ser = unitype_to_multitype(ser, dt);
    df.with_column(new_ser).unwrap();
}

pub fn create_compatible_solution_mappings(
    mut left_mappings: LazyFrame,
    mut left_datatypes: HashMap<String, RDFNodeType>,
    mut right_mappings: LazyFrame,
    mut right_datatypes: HashMap<String, RDFNodeType>,
) -> (
    LazyFrame,
    HashMap<String, RDFNodeType>,
    LazyFrame,
    HashMap<String, RDFNodeType>,
) {
    for (v, dt) in &right_datatypes {
        if let Some(left_dt) = left_datatypes.get(v) {
            if dt != left_dt {
                let mut left_df = left_mappings.collect().unwrap();
                if left_dt != &RDFNodeType::MultiType {
                    convert_df_col_to_multitype(&mut left_df, v, left_dt);
                }
                let mut right_df = right_mappings.collect().unwrap();
                if dt != &RDFNodeType::MultiType {
                    convert_df_col_to_multitype(&mut right_df, v, &dt);
                }
                right_mappings = right_df.lazy();
                left_mappings = left_df.lazy();
                left_datatypes.insert(v.clone(), RDFNodeType::MultiType);
            }
        }
    }
    for (v, dt) in &left_datatypes {
        if right_datatypes.contains_key(v) {
            right_datatypes.insert(v.clone(), dt.clone());
        }
    }
    (
        left_mappings,
        left_datatypes,
        right_mappings,
        right_datatypes,
    )
}

pub fn unitype_to_multitype(ser: &Series, dt: &RDFNodeType) -> Series {
    let out_ser: Series = match dt {
        RDFNodeType::IRI => convert_to_multitype(
            |x: AnyValue| match x {
                AnyValue::Utf8(a) => MultiType::IRI(NamedNode::new_unchecked(a)),
                _ => {
                    panic!()
                }
            },
            ser,
        ),
        RDFNodeType::BlankNode => convert_to_multitype(
            |x: AnyValue| match x {
                AnyValue::Utf8(a) => MultiType::BlankNode(BlankNode::new_unchecked(a)),
                _ => {
                    panic!()
                }
            },
            ser,
        ),
        RDFNodeType::Literal(l) => match l.as_ref() {
            xsd::STRING => convert_to_multitype(
                |x: AnyValue| match x {
                    AnyValue::Utf8(a) => MultiType::Literal(Literal::from(a)),
                    _ => {
                        panic!()
                    }
                },
                ser,
            ),
            xsd::UNSIGNED_INT => convert_to_multitype(
                |x: AnyValue| match x {
                    AnyValue::UInt32(a) => MultiType::Literal(Literal::from(a)),
                    _ => {
                        panic!()
                    }
                },
                ser,
            ),
            xsd::UNSIGNED_LONG => convert_to_multitype(
                |x: AnyValue| match x {
                    AnyValue::UInt64(a) => MultiType::Literal(Literal::from(a)),
                    _ => {
                        panic!()
                    }
                },
                ser,
            ),
            xsd::INTEGER | xsd::LONG => convert_to_multitype(
                |x: AnyValue| match x {
                    AnyValue::Int64(a) => MultiType::Literal(Literal::from(a)),
                    _ => {
                        panic!()
                    }
                },
                ser,
            ),
            xsd::INT => convert_to_multitype(
                |x: AnyValue| match x {
                    AnyValue::Int32(a) => MultiType::Literal(Literal::from(a)),
                    _ => {
                        panic!()
                    }
                },
                ser,
            ),
            xsd::DOUBLE => convert_to_multitype(
                |x: AnyValue| match x {
                    AnyValue::Float64(a) => MultiType::Literal(Literal::from(a)),
                    _ => {
                        panic!()
                    }
                },
                ser,
            ),
            xsd::FLOAT => convert_to_multitype(
                |x: AnyValue| match x {
                    AnyValue::Float32(a) => MultiType::Literal(Literal::from(a)),
                    _ => {
                        panic!()
                    }
                },
                ser,
            ),
            xsd::BOOLEAN => convert_to_multitype(
                |x: AnyValue| match x {
                    AnyValue::Boolean(a) => MultiType::Literal(Literal::from(a)),
                    _ => {
                        panic!()
                    }
                },
                ser,
            ),
            _ => todo!("Not yet implemented: {:?}", dt),
        },
        RDFNodeType::None => convert_to_multitype(|_: AnyValue| MultiType::Null, ser),
        _ => {
            todo!()
        }
    };
    out_ser
}

fn convert_to_multitype(f: fn(AnyValue) -> MultiType, objects: &Series) -> Series {
    let vs = objects.iter().map(|x| f(x));
    let s: ObjectChunked<MultiType> = ChunkedArray::from_iter_values(objects.name(), vs);
    s.into()
}

pub fn multi_series_to_string_series(ser: &Series) -> Series {
    let c = ser.name();
    let mut strs = vec![];
    for i in 0..ser.len() {
        let maybe_o = ser.get_object(i);
        if let Some(o) = maybe_o {
            let o: Option<&MultiType> = o.as_any().downcast_ref();
            if let Some(m) = o {
                strs.push(Some(m.to_string()));
            } else {
                strs.push(None)
            }
        } else {
            strs.push(None);
        }
    }
    let new_ser = Series::new(&c, strs);
    new_ser
}

// Workaround for joins and sorts on MultiType column
pub fn clean_up_after_join_workaround(
    mut solution_mappings: LazyFrame,
    left_original_map: HashMap<String, String>,
    right_original_map: HashMap<String, String>,
) -> LazyFrame {
    for (orig_colname, left_orig_col) in &left_original_map {
        let right_orig_col = right_original_map.get(orig_colname).unwrap();
        solution_mappings = solution_mappings
            .with_column(coalesce(&[col(left_orig_col), col(right_orig_col)]).alias(orig_colname))
            .drop_columns([left_orig_col, right_orig_col]);
    }
    solution_mappings
}

pub fn helper_cols_join_workaround_polars_object_series_bug(
    mut left: LazyFrame,
    mut right: LazyFrame,
    join_on: &Vec<String>,
    left_datatypes: &HashMap<String, RDFNodeType>,
) -> (
    LazyFrame,
    LazyFrame,
    HashMap<String, String>,
    HashMap<String, String>,
) {
    let mut left_original_map = HashMap::new();
    let mut right_original_map = HashMap::new();
    for c in join_on {
        if left_datatypes.get(c).unwrap() == &RDFNodeType::MultiType {
            let left_original = format!("{c}_left_original");
            let right_original = format!("{c}_right_original");
            left = left.with_column(col(c).alias(&left_original));
            right = right.with_column(col(c).alias(&right_original));

            let mut left_df = left.collect().unwrap();
            let mut right_df = right.collect().unwrap();
            let left_col = left_df.column(c).unwrap();
            let right_col = right_df.column(c).unwrap();
            let new_left_col = multi_series_to_string_series(left_col);
            let new_right_col = multi_series_to_string_series(right_col);
            left_df.with_column(new_left_col).unwrap();
            right_df.with_column(new_right_col).unwrap();
            left = left_df.lazy();
            right = right_df.lazy();

            left_original_map.insert(c.clone(), left_original);
            right_original_map.insert(c.clone(), right_original);
        }
    }
    (left, right, left_original_map, right_original_map)
}

pub fn clean_up_after_sort_workaround(
    mut solution_mappings: LazyFrame,
    original_map: HashMap<String, String>,
) -> LazyFrame {
    for (orig_colname, orig_col) in &original_map {
        solution_mappings = solution_mappings
            .with_column(col(orig_col).alias(orig_colname))
            .drop_columns([orig_col]);
    }
    solution_mappings
}

pub fn helper_cols_sort_workaround_polars_object_series_bug(
    mut mappings: LazyFrame,
    datatypes: &HashMap<String, RDFNodeType>,
    expression: &Vec<OrderExpression>,
) -> (LazyFrame, HashMap<String, String>) {
    let mut original_map = HashMap::new();
    let mut order_vars = HashSet::new();
    for oe in expression {
        order_vars.extend(find_oe_columns(oe))
    }

    for (c, dt) in datatypes {
        if order_vars.contains(c) {
            if dt == &RDFNodeType::MultiType {
                let original = format!("{c}_original");
                mappings = mappings.with_column(col(c).alias(&original));

                let mut df = mappings.collect().unwrap();
                let col = df.column(c).unwrap();
                let new_col = multi_series_to_string_series(col);
                df.with_column(new_col).unwrap();
                mappings = df.lazy();

                original_map.insert(c.clone(), original);
            }
        }
    }
    (mappings, original_map)
}

fn find_oe_columns(oe: &OrderExpression) -> Vec<String> {
    match oe {
        OrderExpression::Asc(a) | OrderExpression::Desc(a) => find_e_columns(a),
    }
}

fn find_e_columns(e: &Expression) -> Vec<String> {
    match e {
        Expression::NamedNode(_) | Expression::Literal(_) => {
            vec![]
        }
        Expression::Bound(v) | Expression::Variable(v) => {
            vec![v.as_str().to_string()]
        }
        Expression::Divide(a, b)
        | Expression::Multiply(a, b)
        | Expression::Subtract(a, b)
        | Expression::Add(a, b)
        | Expression::LessOrEqual(a, b)
        | Expression::Less(a, b)
        | Expression::GreaterOrEqual(a, b)
        | Expression::Greater(a, b)
        | Expression::SameTerm(a, b)
        | Expression::Or(a, b)
        | Expression::And(a, b)
        | Expression::Equal(a, b) => {
            let mut out = find_e_columns(a);
            out.extend(find_e_columns(b));
            out
        },
        Expression::In(a, bs) => {
            let mut out_a = find_e_columns(a);
            for b in bs {
                out_a.extend(find_e_columns(b));
            }
            out_a
        }
        Expression::Not(a) | Expression::UnaryPlus(a) | Expression::UnaryMinus(a) => {
            find_e_columns(a)
        }
        Expression::Exists(q) => {
            todo!()
        }
        Expression::If(a, b, c) => {
            let mut out = find_e_columns(a);
            out.extend(find_e_columns(b));
            out.extend(find_e_columns(c));
            out
        }
        Expression::FunctionCall(_, es) | Expression::Coalesce(es) => {
            let mut out = vec![];
            for e in es {
                out.extend(find_e_columns(e))
            }
            out
        }
    }
}
