use crate::sparql::sparql_to_polars::{
    polars_literal_values_to_series, sparql_blank_node_to_polars_literal_value,
    sparql_literal_to_polars_literal_value, sparql_named_node_to_polars_literal_value,
};
use oxrdf::vocab::xsd;
use oxrdf::{BlankNode, Literal, NamedNode, Subject, Term};
use polars::export::arrow::util::total_ord::TotalEq;
use polars::prelude::{coalesce, col, lit, IntoLazy, LazyFrame, LiteralValue};
use polars_core::frame::DataFrame;
use polars_core::prelude::{
    AnyValue, ChunkedArray, DataType, IntoSeries, NamedFrom, NewChunkedArray, ObjectChunked,
    PolarsObject, SortOptions,
};
use polars_core::series::Series;
use representation::{literal_blanknode_to_blanknode, literal_iri_to_namednode, RDFNodeType};
use spargebra::algebra::{Expression, OrderExpression};
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};
use std::hash::Hash;
use std::hash::Hasher;

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

impl From<Subject> for MultiType {
    fn from(s: Subject) -> Self {
        match s {
            Subject::NamedNode(nn) => MultiType::IRI(nn),
            Subject::BlankNode(bn) => MultiType::BlankNode(bn),
        }
    }
}

impl From<Term> for MultiType {
    fn from(t: Term) -> Self {
        match t {
            Term::NamedNode(nn) => MultiType::IRI(nn),
            Term::BlankNode(bn) => MultiType::BlankNode(bn),
            Term::Literal(l) => MultiType::Literal(l),
        }
    }
}

impl From<Literal> for MultiType {
    fn from(value: Literal) -> Self {
        MultiType::Literal(value)
    }
}

impl From<NamedNode> for MultiType {
    fn from(value: NamedNode) -> Self {
        MultiType::IRI(value.clone())
    }
}

impl From<BlankNode> for MultiType {
    fn from(value: BlankNode) -> Self {
        MultiType::BlankNode(value)
    }
}

impl TotalEq for MultiType {
    fn tot_eq(&self, other: &Self) -> bool {
        let mut s = DefaultHasher::new();
        let mut t = DefaultHasher::new();
        self.hash(&mut s);
        other.hash(&mut t);
        s.finish() == t.finish()
    }

    fn tot_ne(&self, other: &Self) -> bool {
        !self.tot_eq(other)
    }
}

impl PolarsObject for MultiType {
    fn type_name() -> &'static str {
        MULTI_TYPE_NAME
    }
}

pub fn convert_df_col_to_multitype(df: &mut DataFrame, col: &str, dt: &RDFNodeType) {
    if dt == &RDFNodeType::MultiType {
        return;
    }
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

pub fn create_join_compatible_solution_mappings(
    mut left_mappings: LazyFrame,
    mut left_datatypes: HashMap<String, RDFNodeType>,
    mut right_mappings: LazyFrame,
    mut right_datatypes: HashMap<String, RDFNodeType>,
    inner: bool,
) -> (
    LazyFrame,
    HashMap<String, RDFNodeType>,
    LazyFrame,
    HashMap<String, RDFNodeType>,
) {
    let mut new_left_datatypes = HashMap::new();
    let mut new_right_datatypes = HashMap::new();
    for (v, right_dt) in &right_datatypes {
        if let Some(left_dt) = left_datatypes.get(v) {
            if right_dt != left_dt {
                if left_dt == &RDFNodeType::MultiType {
                    if inner {
                        let mut left_df = left_mappings.collect().unwrap();
                        force_convert_df_multicol_to_single_col(&mut left_df, v, right_dt);
                        left_mappings = left_df.lazy();
                        new_left_datatypes.insert(v.clone(), right_dt.clone());
                    } else {
                        let mut right_df = right_mappings.collect().unwrap();
                        convert_df_col_to_multitype(&mut right_df, v, right_dt);
                        right_mappings = right_df.lazy();
                        new_right_datatypes.insert(v.clone(), RDFNodeType::MultiType);
                    }
                } else {
                    if right_dt == &RDFNodeType::MultiType {
                        let mut right_df = right_mappings.collect().unwrap();
                        force_convert_df_multicol_to_single_col(&mut right_df, v, left_dt);
                        right_mappings = right_df.lazy();
                        new_right_datatypes.insert(v.clone(), left_dt.clone());
                    } else {
                        right_mappings = right_mappings
                            .drop_columns([v])
                            .with_column(lit(LiteralValue::Null).cast(left_dt.polars_data_type()).alias(v));
                        new_right_datatypes.insert(v.clone(), left_dt.clone());
                    }
                }
            }
        }
    }
    left_datatypes.extend(new_left_datatypes);
    right_datatypes.extend(new_right_datatypes);
    (
        left_mappings,
        left_datatypes,
        right_mappings,
        right_datatypes,
    )
}

pub fn force_convert_df_multicol_to_single_col(df: &mut DataFrame, c: &str, dt: &RDFNodeType) {
    let ser = df.drop_in_place(c).unwrap();
    let mut new = vec![];
    for i in 0..ser.len() {
        let o = ser.get_object(i);
        if let Some(o) = o {
            let m: &MultiType = o.as_any().downcast_ref().unwrap();
            let matches = match m {
                MultiType::IRI(_) => dt == &RDFNodeType::IRI,
                MultiType::BlankNode(_) => dt == &RDFNodeType::BlankNode,
                MultiType::Literal(l) => {
                    if let RDFNodeType::Literal(nn) = dt {
                        nn.as_ref() == l.datatype()
                    } else {
                        false
                    }
                }
                MultiType::Null => false,
            };
            new.push(if matches { Some(m.clone()) } else { None });
        } else {
            new.push(None)
        }
    }
    let och = ObjectChunked::from_iter_options(ser.name(), new.into_iter());
    let new_ser = och.into_series();
    df.with_column(new_ser).unwrap();
    maybe_convert_df_multicol_to_single(df, c, false);
}

pub fn unitype_to_multitype(ser: &Series, dt: &RDFNodeType) -> Series {
    let ser = if let DataType::Categorical(_) = ser.dtype() {
        ser.cast(&DataType::Utf8).unwrap()
    } else {
        ser.clone()
    };
    let out_ser: Series = match dt {
        RDFNodeType::IRI => convert_to_multitype(
            |x: AnyValue| match x {
                AnyValue::Utf8(a) => MultiType::IRI(literal_iri_to_namednode(a)),
                _ => {
                    panic!()
                }
            },
            &ser,
        ),
        RDFNodeType::BlankNode => convert_to_multitype(
            |x: AnyValue| match x {
                AnyValue::Utf8(a) => MultiType::BlankNode(literal_blanknode_to_blanknode(a)),
                _ => {
                    panic!()
                }
            },
            &ser,
        ),
        RDFNodeType::Literal(l) => match l.as_ref() {
            xsd::STRING => convert_to_multitype(
                |x: AnyValue| match x {
                    AnyValue::Utf8(a) => MultiType::Literal(Literal::from(a)),
                    _ => {
                        panic!()
                    }
                },
                &ser,
            ),
            xsd::BYTE => convert_to_multitype(
                |x: AnyValue| match x {
                    AnyValue::Int8(i) => {
                        MultiType::Literal(Literal::new_typed_literal(i.to_string(), xsd::BYTE))
                    }
                    _ => {
                        panic!()
                    }
                },
                &ser,
            ),
            xsd::SHORT => convert_to_multitype(
                |x: AnyValue| match x {
                    AnyValue::UInt16(i) => MultiType::Literal(Literal::from(i)),
                    _ => {
                        panic!()
                    }
                },
                &ser,
            ),
            xsd::UNSIGNED_BYTE => convert_to_multitype(
                |x: AnyValue| match x {
                    AnyValue::UInt8(u) => MultiType::Literal(Literal::new_typed_literal(
                        u.to_string(),
                        xsd::UNSIGNED_BYTE,
                    )),
                    _ => {
                        panic!()
                    }
                },
                &ser,
            ),
            xsd::UNSIGNED_SHORT => convert_to_multitype(
                |x: AnyValue| match x {
                    AnyValue::UInt16(u) => MultiType::Literal(Literal::from(u)),
                    _ => {
                        panic!()
                    }
                },
                &ser,
            ),
            xsd::UNSIGNED_INT => convert_to_multitype(
                |x: AnyValue| match x {
                    AnyValue::UInt32(a) => MultiType::Literal(Literal::from(a)),
                    _ => {
                        panic!()
                    }
                },
                &ser,
            ),
            xsd::UNSIGNED_LONG => convert_to_multitype(
                |x: AnyValue| match x {
                    AnyValue::UInt64(a) => MultiType::Literal(Literal::from(a)),
                    _ => {
                        panic!()
                    }
                },
                &ser,
            ),
            xsd::INTEGER | xsd::LONG => convert_to_multitype(
                |x: AnyValue| match x {
                    AnyValue::Int64(a) => MultiType::Literal(Literal::from(a)),
                    _ => {
                        panic!("{:?}", x)
                    }
                },
                &ser,
            ),
            xsd::NON_NEGATIVE_INTEGER => convert_to_multitype(
                |x: AnyValue| match x {
                    AnyValue::UInt64(a) => MultiType::Literal(Literal::from(a)),
                    _ => {
                        panic!("{:?}", x)
                    }
                },
                &ser,
            ),
            xsd::INT => convert_to_multitype(
                |x: AnyValue| match x {
                    AnyValue::Int32(a) => MultiType::Literal(Literal::from(a)),
                    _ => {
                        panic!()
                    }
                },
                &ser,
            ),
            xsd::DOUBLE => convert_to_multitype(
                |x: AnyValue| match x {
                    AnyValue::Float64(a) => MultiType::Literal(Literal::from(a)),
                    _ => {
                        panic!()
                    }
                },
                &ser,
            ),
            xsd::FLOAT => convert_to_multitype(
                |x: AnyValue| match x {
                    AnyValue::Float32(a) => MultiType::Literal(Literal::from(a)),
                    _ => {
                        panic!()
                    }
                },
                &ser,
            ),
            xsd::BOOLEAN => convert_to_multitype(
                |x: AnyValue| match x {
                    AnyValue::Boolean(a) => MultiType::Literal(Literal::from(a)),
                    _ => {
                        panic!()
                    }
                },
                &ser,
            ),
            xsd::DECIMAL => convert_to_multitype(
                |x: AnyValue| match x {
                    AnyValue::Float64(f) => {
                        MultiType::Literal(Literal::new_typed_literal(f.to_string(), xsd::DECIMAL))
                    }
                    _ => panic!("anyvalue {:?}", x),
                },
                &ser,
            ),
            _ => todo!("Not yet implemented: {:?}", dt),
        },
        RDFNodeType::None => convert_to_multitype(|_: AnyValue| MultiType::Null, &ser),
        RDFNodeType::MultiType => ser.clone(),
    };
    out_ser
}

fn convert_to_multitype(f: fn(AnyValue) -> MultiType, objects: &Series) -> Series {
    let vs = objects.iter().map(|x| match x {
        AnyValue::Null => None,
        x => Some(f(x)),
    });
    let vs: Vec<_> = vs.collect();
    let s: ObjectChunked<MultiType> =
        ChunkedArray::from_iter_options(objects.name(), vs.into_iter());
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
        }
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
        Expression::Exists(_q) => {
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

fn multi_to_polars_literal_value(m: &MultiType) -> LiteralValue {
    match m {
        MultiType::IRI(i) => sparql_named_node_to_polars_literal_value(i),
        MultiType::BlankNode(b) => sparql_blank_node_to_polars_literal_value(b),
        MultiType::Literal(l) => sparql_literal_to_polars_literal_value(l),
        MultiType::Null => {
            panic!("")
        }
    }
}

pub fn maybe_convert_df_multicol_to_single(df: &mut DataFrame, c: &str, maybe: bool) -> bool {
    let c_ser = df.column(c).unwrap();
    if let DataType::Object(_) = c_ser.dtype() {
    } else {
        return true;
    }
    let mut first_datatype = None;
    for i in 0..c_ser.len() {
        if !maybe && first_datatype.is_some() {
            break;
        }
        let c_obj = c_ser.get_object(i);
        if let Some(c_obj) = c_obj {
            let c_obj: &MultiType = c_obj.as_any().downcast_ref().unwrap();
            match c_obj {
                MultiType::IRI(_i) => {
                    if let Some(dt) = &first_datatype {
                        if dt != &RDFNodeType::IRI {
                            return false;
                        }
                    } else {
                        first_datatype = Some(RDFNodeType::IRI)
                    }
                }
                MultiType::BlankNode(_b) => {
                    if let Some(dt) = &first_datatype {
                        if dt != &RDFNodeType::BlankNode {
                            return false;
                        }
                    } else {
                        first_datatype = Some(RDFNodeType::BlankNode)
                    }
                }
                MultiType::Literal(l) => {
                    let dt = l.datatype();
                    if let Some(rdfnodetype) = &first_datatype {
                        if let RDFNodeType::Literal(otherdt) = rdfnodetype {
                            if dt != otherdt.as_ref() {
                                return false;
                            }
                        } else {
                            return false;
                        }
                    } else {
                        first_datatype = Some(RDFNodeType::Literal(dt.into_owned()))
                    }
                }
                MultiType::Null => {
                    panic!("")
                }
            }
        }
    }
    if let Some(_dt) = first_datatype {
        let mut literal_values = vec![];
        for i in 0..c_ser.len() {
            let o: Option<&MultiType> = c_ser.get_object(i).unwrap().as_any().downcast_ref();
            literal_values.push(match o {
                None => LiteralValue::Null,
                Some(m) => multi_to_polars_literal_value(m),
            })
        }
        let new_series = polars_literal_values_to_series(literal_values, c);
        df.with_column(new_series).unwrap();
    } else {
        df.with_column(
            Series::new_null(c, df.height())
                .cast(&DataType::Utf8)
                .unwrap(),
        )
        .unwrap();
    }
    true
}

pub fn split_df_multicol(df: &mut DataFrame, c: &str) -> Vec<(DataFrame, RDFNodeType)> {
    let c_ser = df.column(c).unwrap();
    let mut datatypes_vec = vec![];
    for i in 0..c_ser.len() {
        let c_obj: Option<&MultiType> = c_ser.get_object(i).unwrap().as_any().downcast_ref();
        if let Some(c_obj) = c_obj {
            match c_obj {
                MultiType::IRI(_i) => {
                    datatypes_vec.push("i");
                }
                MultiType::BlankNode(_b) => {
                    datatypes_vec.push("b");
                }
                MultiType::Literal(l) => {
                    datatypes_vec.push(l.datatype().as_str());
                }
                MultiType::Null => {
                    panic!("")
                }
            }
        } else {
            datatypes_vec.push("n");
        }
    }
    let key_col_name = "key_col".to_string();
    let s = Series::new(&key_col_name, datatypes_vec);
    df.with_column(s)
        .unwrap()
        .sort_in_place(vec![&key_col_name], vec![false], true)
        .unwrap();
    let mut dfs = df
        .select([&key_col_name, c])
        .unwrap()
        .partition_by_stable(vec![&key_col_name], true)
        .unwrap();
    dfs.sort_by_key(|x| {
        x.column(&key_col_name)
            .unwrap()
            .utf8()
            .unwrap()
            .get(0)
            .unwrap()
            .to_string()
    });
    let mut dfs_dts = vec![];
    for mut df in dfs {
        let s = df.drop_in_place(&key_col_name).unwrap();
        let dt = match s.utf8().unwrap().get(0).unwrap() {
            "i" => RDFNodeType::IRI,
            "b" => RDFNodeType::BlankNode,
            t => RDFNodeType::Literal(NamedNode::new_unchecked(t)),
        };
        maybe_convert_df_multicol_to_single(&mut df, c, false);
        dfs_dts.push((df, dt));
    }
    let _ = df.drop_in_place(&key_col_name).unwrap();
    dfs_dts
}

pub fn split_df_multicols(
    mut df: DataFrame,
    cs: Vec<&str>,
) -> Vec<(DataFrame, HashMap<String, RDFNodeType>)> {
    let mut key_cols = vec![];
    for (i, c) in cs.iter().enumerate() {
        let c_ser = df.column(c).unwrap();
        let mut datatypes_vec = vec![];
        for i in 0..c_ser.len() {
            let c_opt = c_ser.get_object(i);
            if let Some(c_obj) = c_opt {
                let c_obj: &MultiType = c_obj.as_any().downcast_ref().unwrap();
                match c_obj {
                    MultiType::IRI(_i) => {
                        datatypes_vec.push("i");
                    }
                    MultiType::BlankNode(_b) => {
                        datatypes_vec.push("b");
                    }
                    MultiType::Literal(l) => {
                        datatypes_vec.push(l.datatype().as_str());
                    }
                    MultiType::Null => {
                        panic!("")
                    }
                }
            } else {
                datatypes_vec.push("i");
            }
        }
        let key_col_name = format!("key_col_{i}");
        let s = Series::new(&key_col_name, datatypes_vec);
        key_cols.push(key_col_name);
        df.with_column(s).unwrap();
    }
    let mut concat_expr = col(key_cols.get(0).unwrap());
    for i in 1..key_cols.len() {
        concat_expr = concat_expr + lit("-") + col(key_cols.get(i).unwrap());
    }

    df = df
        .lazy()
        .with_column(concat_expr.alias("key_col"))
        .sort(
            "key_col",
            SortOptions {
                descending: false,
                nulls_last: false,
                multithreaded: true,
                maintain_order: false,
            },
        )
        .collect()
        .unwrap();
    let mut selection = cs.clone();
    selection.push("key_col");
    for k in &key_cols {
        selection.push(k.as_str());
    }

    let dfs = df.partition_by(vec!["key_col"], false).unwrap();
    let mut dfs_dts = vec![];
    for mut df in dfs {
        let mut map = HashMap::new();
        for (i, key_col) in key_cols.iter().enumerate() {
            let s = df.drop_in_place(key_col).unwrap();
            let dt = match s.utf8().unwrap().get(0).unwrap() {
                "i" => RDFNodeType::IRI,
                "b" => RDFNodeType::BlankNode,
                t => RDFNodeType::Literal(NamedNode::new_unchecked(t)),
            };
            maybe_convert_df_multicol_to_single(&mut df, cs.get(i).unwrap(), false);
            map.insert(cs.get(i).unwrap().to_string(), dt);
        }
        dfs_dts.push((df, map));
    }
    dfs_dts
}
