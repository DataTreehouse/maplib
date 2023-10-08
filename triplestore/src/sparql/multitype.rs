use std::fmt::{Display, Formatter};
use oxrdf::{BlankNode, Literal, NamedNode};
use oxrdf::vocab::xsd;
use polars_core::prelude::{AnyValue, ChunkedArray, NewChunkedArray, ObjectChunked, PolarsObject};
use polars_core::series::Series;
use representation::RDFNodeType;

#[derive(Debug, Clone, Default, Eq, Hash, PartialEq)]
pub enum MultiType {
    IRI(NamedNode),
    BlankNode(BlankNode),
    Literal(Literal),
    #[default]
    Null
}

impl Display for MultiType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            MultiType::IRI(i) => {
                write!(f, "IRI({})", i)
            }
            MultiType::BlankNode(b) => {
                write!(f, "BlankNode({})", b)
            }
            MultiType::Literal(l) => {
                write!(f, "IRI({})", l)
            }
            MultiType::Null => {
                write!(f, "Null")
            }
        }
    }
}

impl PolarsObject for MultiType {
    fn type_name() -> &'static str {
        todo!()
    }
}

pub fn unitype_to_multitype(ser:&Series, dt:&RDFNodeType) -> Series {
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
                _ => todo!("Not yet implemented: {:?}", dt)
            },
            RDFNodeType::None => {
                todo!()
            }
            _ => {
                todo!()
            }
        };
    out_ser
}


fn convert_to_multitype(f: fn(AnyValue) -> MultiType, objects: &Series) -> Series {
    let vs = objects.iter().map(|x| f(x));
    let s: ObjectChunked<MultiType> = ChunkedArray::from_iter_values("object", vs);
    s.into()
}