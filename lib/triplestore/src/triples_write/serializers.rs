// The following code is based on the code below from Polars by Ritchie Vink and other contributors.
// https://github.com/pola-rs/polars/blob/dc94be767d26943be11a40d6171ccc1c41a86c4f/crates/polars-io/src/csv/write/write_impl/serializer.rs
// The Polars license can be found in the licensing folder.

use polars_core::prelude::*;
use polars_core::utils::arrow::array::{Array, BooleanArray, PrimitiveArray, Utf8ViewArray};
use polars_core::utils::arrow::types::NativeType;
use representation::BaseRDFNodeType;
use std::io::Write;

const TOO_MANY_MSG: &str = "too many items requested from CSV serializer";
const ARRAY_MISMATCH_MSG: &str = "wrong array type";

const QUOTE_CHAR: u8 = b'"';

#[allow(dead_code)]
struct IgnoreFmt;
impl std::fmt::Write for IgnoreFmt {
    fn write_str(&mut self, _s: &str) -> std::fmt::Result {
        Ok(())
    }
}

pub(super) trait Serializer<'a> {
    fn serialize(&mut self, buf: &mut Vec<u8>);
    // Updates the array without changing the configuration.
    fn update_array(&mut self, array: &'a dyn Array);
}

fn make_serializer<'a, T, I: Iterator<Item = Option<T>>>(
    f: impl FnMut(T, &mut Vec<u8>),
    iter: I,
    update_array: impl FnMut(&'a dyn Array) -> I,
) -> impl Serializer<'a> {
    struct SerializerImpl<F, I, Update> {
        f: F,
        iter: I,
        update_array: Update,
    }

    impl<'a, T, F, I, Update> Serializer<'a> for SerializerImpl<F, I, Update>
    where
        F: FnMut(T, &mut Vec<u8>),
        I: Iterator<Item = Option<T>>,
        Update: FnMut(&'a dyn Array) -> I,
    {
        fn serialize(&mut self, buf: &mut Vec<u8>) {
            let item = self.iter.next().expect(TOO_MANY_MSG);
            if let Some(item) = item {
                (self.f)(item, buf);
            }
        }

        fn update_array(&mut self, array: &'a dyn Array) {
            self.iter = (self.update_array)(array);
        }
    }

    SerializerImpl::<_, _, _> {
        f,
        iter,
        update_array,
    }
}

fn integer_serializer<I: NativeType + itoa::Integer>(array: &PrimitiveArray<I>) -> impl Serializer {
    let f = move |&item, buf: &mut Vec<u8>| {
        let mut buffer = itoa::Buffer::new();
        let value = buffer.format(item);
        buf.extend_from_slice(value.as_bytes());
    };

    make_serializer::<_, _>(f, array.iter(), |array| {
        array
            .as_any()
            .downcast_ref::<PrimitiveArray<I>>()
            .expect(ARRAY_MISMATCH_MSG)
            .iter()
    })
}

fn float_serializer_no_precision_autoformat<I: NativeType + ryu::Float>(
    array: &PrimitiveArray<I>,
) -> impl Serializer {
    let f = move |&item, buf: &mut Vec<u8>| {
        let mut buffer = ryu::Buffer::new();
        let value = buffer.format(item);
        buf.extend_from_slice(value.as_bytes());
    };

    make_serializer::<_, _>(f, array.iter(), |array| {
        array
            .as_any()
            .downcast_ref::<PrimitiveArray<I>>()
            .expect(ARRAY_MISMATCH_MSG)
            .iter()
    })
}

fn bool_serializer(array: &BooleanArray) -> impl Serializer {
    let f = move |item, buf: &mut Vec<u8>| {
        let s = if item { "true" } else { "false" };
        buf.extend_from_slice(s.as_bytes());
    };

    make_serializer::<_, _>(f, array.iter(), |array| {
        array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect(ARRAY_MISMATCH_MSG)
            .iter()
    })
}

pub(super) fn string_serializer<'a, Iter: Send + 'a>(
    mut f: impl FnMut(&mut Iter) -> Option<&str> + Send + 'a,
    mut update: impl FnMut(&'a dyn Array) -> Iter + Send + 'a,
    array: &'a dyn Array,
) -> Box<dyn Serializer<'a> + 'a + Send> {
    struct StringSerializer<F, Iter, Update> {
        serialize: F,
        update: Update,
        iter: Iter,
    }

    impl<'a, F, Iter, Update> Serializer<'a> for StringSerializer<F, Iter, Update>
    where
        F: FnMut(&mut Iter, &mut Vec<u8>),
        Update: FnMut(&'a dyn Array) -> Iter,
    {
        fn serialize(&mut self, buf: &mut Vec<u8>) {
            (self.serialize)(&mut self.iter, buf);
        }

        fn update_array(&mut self, array: &'a dyn Array) {
            self.iter = (self.update)(array);
        }
    }

    fn write_escaped_char(c: char, f: &mut Vec<u8>) {
        match c {
            '\n' => {
                write!(f, "\\n").unwrap();
            }
            '\t' => {
                write!(f, "\\t").unwrap();
            }
            '\r' => {
                write!(f, "\\r").unwrap();
            }
            '"' | '\\' => {
                write!(f, "\\{c}").unwrap();
            }
            _ => {
                write!(f, "{c}").unwrap();
            }
        }
    }

    let iter = update(array);
    let serialize = move |iter: &mut Iter, buf: &mut Vec<u8>| {
        buf.push(QUOTE_CHAR);
        let Some(s) = f(iter) else {
            return;
        };
        let chars = s.chars();
        for c in chars {
            write_escaped_char(c, buf);
        }
        buf.push(QUOTE_CHAR);
    };
    Box::new(StringSerializer {
        serialize,
        update,
        iter,
    })
}

pub(super) fn lang_serializer<'a, Iter: Send + 'a>(
    mut f: impl FnMut(&mut Iter) -> Option<&str> + Send + 'a,
    mut update: impl FnMut(&'a dyn Array) -> Iter + Send + 'a,
    array: &'a dyn Array,
) -> Box<dyn Serializer<'a> + 'a + Send> {
    const LANG_CHAR: u8 = b'@';

    struct StringSerializer<F, Iter, Update> {
        serialize: F,
        update: Update,
        iter: Iter,
    }

    impl<'a, F, Iter, Update> Serializer<'a> for StringSerializer<F, Iter, Update>
    where
        F: FnMut(&mut Iter, &mut Vec<u8>),
        Update: FnMut(&'a dyn Array) -> Iter,
    {
        fn serialize(&mut self, buf: &mut Vec<u8>) {
            (self.serialize)(&mut self.iter, buf);
        }

        fn update_array(&mut self, array: &'a dyn Array) {
            self.iter = (self.update)(array);
        }
    }

    let iter = update(array);
    let serialize = move |iter: &mut Iter, buf: &mut Vec<u8>| {
        buf.push(LANG_CHAR);
        let Some(s) = f(iter) else {
            return;
        };
        buf.extend_from_slice(s.as_bytes());
    };
    Box::new(StringSerializer {
        serialize,
        update,
        iter,
    })
}

pub(super) fn blank_node_serializer<'a, Iter: Send + 'a>(
    mut f: impl FnMut(&mut Iter) -> Option<&str> + Send + 'a,
    mut update: impl FnMut(&'a dyn Array) -> Iter + Send + 'a,
    array: &'a dyn Array,
) -> Box<dyn Serializer<'a> + 'a + Send> {
    const BLANK_NODE_PREFIX: [u8; 2] = [b'_', b':'];

    struct StringSerializer<F, Iter, Update> {
        serialize: F,
        update: Update,
        iter: Iter,
    }

    impl<'a, F, Iter, Update> Serializer<'a> for StringSerializer<F, Iter, Update>
    where
        F: FnMut(&mut Iter, &mut Vec<u8>),
        Update: FnMut(&'a dyn Array) -> Iter,
    {
        fn serialize(&mut self, buf: &mut Vec<u8>) {
            (self.serialize)(&mut self.iter, buf);
        }

        fn update_array(&mut self, array: &'a dyn Array) {
            self.iter = (self.update)(array);
        }
    }

    fn serialize_blank_node(buf: &mut Vec<u8>, s: &[u8]) {
        buf.extend_from_slice(&BLANK_NODE_PREFIX);
        buf.extend_from_slice(s);
    }

    let iter = update(array);
    let serialize = move |iter: &mut Iter, buf: &mut Vec<u8>| {
        let Some(s) = f(iter) else {
            return;
        };
        serialize_blank_node(buf, s.as_bytes());
    };
    Box::new(StringSerializer {
        serialize,
        update,
        iter,
    })
}

pub(super) fn iri_serializer<'a, Iter: Send + 'a>(
    mut f: impl FnMut(&mut Iter) -> Option<&str> + Send + 'a,
    mut update: impl FnMut(&'a dyn Array) -> Iter + Send + 'a,
    array: &'a dyn Array,
) -> Box<dyn Serializer<'a> + 'a + Send> {
    const LEFT: u8 = b'<';
    const RIGHT: u8 = b'>';

    struct StringSerializer<F, Iter, Update> {
        serialize: F,
        update: Update,
        iter: Iter,
    }

    impl<'a, F, Iter, Update> Serializer<'a> for StringSerializer<F, Iter, Update>
    where
        F: FnMut(&mut Iter, &mut Vec<u8>),
        Update: FnMut(&'a dyn Array) -> Iter,
    {
        fn serialize(&mut self, buf: &mut Vec<u8>) {
            (self.serialize)(&mut self.iter, buf);
        }

        fn update_array(&mut self, array: &'a dyn Array) {
            self.iter = (self.update)(array);
        }
    }

    let iter = update(array);
    let serialize = move |iter: &mut Iter, buf: &mut Vec<u8>| {
        let Some(s) = f(iter) else {
            return;
        };
        buf.push(LEFT);
        buf.extend_from_slice(s.as_bytes());
        buf.push(RIGHT);
    };
    Box::new(StringSerializer {
        serialize,
        update,
        iter,
    })
}

fn quoted_serializer<'a>(serializer: impl Serializer<'a>) -> impl Serializer<'a> {
    struct QuoteSerializer<S>(S);
    impl<'a, S: Serializer<'a>> Serializer<'a> for QuoteSerializer<S> {
        fn serialize(&mut self, buf: &mut Vec<u8>) {
            buf.push(QUOTE_CHAR);
            self.0.serialize(buf);
            buf.push(QUOTE_CHAR);
        }

        fn update_array(&mut self, array: &'a dyn Array) {
            self.0.update_array(array);
        }
    }
    QuoteSerializer(serializer)
}

pub(super) fn serializer_for<'a>(
    array: &'a dyn Array,
    dtype: &'a DataType,
    rdf_node_type: &'a BaseRDFNodeType,
    is_lang_tag: bool,
) -> PolarsResult<Box<dyn Serializer<'a> + Send + 'a>> {
    macro_rules! with_quoted_serializer {
        ($make_serializer:path, $($arg:tt)*) => {{
            let serializer = $make_serializer(array.as_any().downcast_ref().unwrap(), $($arg)*);
            Box::new(quoted_serializer(serializer)) as Box<dyn Serializer + Send>
        }};
        ($make_serializer:path) => { with_quoted_serializer!($make_serializer,) };
    }
    let serializer = match (dtype, rdf_node_type) {
        (DataType::Int8, _) => with_quoted_serializer!(integer_serializer::<i8>),
        (DataType::UInt8, _) => with_quoted_serializer!(integer_serializer::<u8>),
        (DataType::Int16, _) => with_quoted_serializer!(integer_serializer::<i16>),
        (DataType::UInt16, _) => with_quoted_serializer!(integer_serializer::<u16>),
        (DataType::Int32, _) => with_quoted_serializer!(integer_serializer::<i32>),
        (DataType::UInt32, _) => with_quoted_serializer!(integer_serializer::<u32>),
        (DataType::Int64, _) => with_quoted_serializer!(integer_serializer::<i64>),
        (DataType::UInt64, _) => with_quoted_serializer!(integer_serializer::<u64>),
        (DataType::Float32, _) => {
            with_quoted_serializer!(float_serializer_no_precision_autoformat::<f32>)
        }
        (DataType::Float64, _) => {
            with_quoted_serializer!(float_serializer_no_precision_autoformat::<f64>)
        }
        (DataType::Boolean, _) => {
            with_quoted_serializer!(bool_serializer)
        }
        (DataType::String, BaseRDFNodeType::BlankNode) => blank_node_serializer(
            |iter| Iterator::next(iter).expect(TOO_MANY_MSG),
            |arr| {
                arr.as_any()
                    .downcast_ref::<Utf8ViewArray>()
                    .expect(ARRAY_MISMATCH_MSG)
                    .iter()
            },
            array,
        ),
        (DataType::String, BaseRDFNodeType::IRI) => iri_serializer(
            |iter| Iterator::next(iter).expect(TOO_MANY_MSG),
            |arr| {
                arr.as_any()
                    .downcast_ref::<Utf8ViewArray>()
                    .expect(ARRAY_MISMATCH_MSG)
                    .iter()
            },
            array,
        ),
        (DataType::String, BaseRDFNodeType::Literal(_)) if is_lang_tag => lang_serializer(
            |iter| Iterator::next(iter).expect(TOO_MANY_MSG),
            |arr| {
                arr.as_any()
                    .downcast_ref::<Utf8ViewArray>()
                    .expect(ARRAY_MISMATCH_MSG)
                    .iter()
            },
            array,
        ),
        (DataType::String, BaseRDFNodeType::Literal(_)) => string_serializer(
            |iter| Iterator::next(iter).expect(TOO_MANY_MSG),
            |arr| {
                arr.as_any()
                    .downcast_ref::<Utf8ViewArray>()
                    .expect(ARRAY_MISMATCH_MSG)
                    .iter()
            },
            array,
        ),
        (
            DataType::Categorical(rev_map, _) | DataType::Enum(rev_map, _),
            BaseRDFNodeType::BlankNode,
        ) => {
            let rev_map = rev_map.as_deref().unwrap();
            blank_node_serializer(
                |iter| {
                    let &idx: &u32 = Iterator::next(iter).expect(TOO_MANY_MSG)?;
                    Some(rev_map.get(idx))
                },
                |arr| {
                    arr.as_any()
                        .downcast_ref::<PrimitiveArray<u32>>()
                        .expect(ARRAY_MISMATCH_MSG)
                        .iter()
                },
                array,
            )
        }
        (DataType::Categorical(rev_map, _) | DataType::Enum(rev_map, _), BaseRDFNodeType::IRI) => {
            let rev_map = rev_map.as_deref().unwrap();
            iri_serializer(
                |iter| {
                    let &idx: &u32 = Iterator::next(iter).expect(TOO_MANY_MSG)?;
                    Some(rev_map.get(idx))
                },
                |arr| {
                    arr.as_any()
                        .downcast_ref::<PrimitiveArray<u32>>()
                        .expect(ARRAY_MISMATCH_MSG)
                        .iter()
                },
                array,
            )
        }
        (
            DataType::Categorical(rev_map, _) | DataType::Enum(rev_map, _),
            BaseRDFNodeType::Literal(_),
        ) if is_lang_tag => {
            let rev_map = rev_map.as_deref().unwrap();
            lang_serializer(
                |iter| {
                    let &idx: &u32 = Iterator::next(iter).expect(TOO_MANY_MSG)?;
                    Some(rev_map.get(idx))
                },
                |arr| {
                    arr.as_any()
                        .downcast_ref::<PrimitiveArray<u32>>()
                        .expect(ARRAY_MISMATCH_MSG)
                        .iter()
                },
                array,
            )
        }
        (
            DataType::Categorical(rev_map, _) | DataType::Enum(rev_map, _),
            BaseRDFNodeType::Literal(_),
        ) => {
            let rev_map = rev_map.as_deref().unwrap();
            string_serializer(
                |iter| {
                    let &idx: &u32 = Iterator::next(iter).expect(TOO_MANY_MSG)?;
                    Some(rev_map.get(idx))
                },
                |arr| {
                    arr.as_any()
                        .downcast_ref::<PrimitiveArray<u32>>()
                        .expect(ARRAY_MISMATCH_MSG)
                        .iter()
                },
                array,
            )
        }
        _ => {
            panic!("datatype {dtype} cannot be written to triples")
        }
    };
    Ok(serializer)
}
