// The following code is based on the code below from Polars by Ritchie Vink and other contributors.
// https://github.com/pola-rs/polars/blob/dc94be767d26943be11a40d6171ccc1c41a86c4f/crates/polars-io/src/csv/write/write_impl/serializer.rs
// The Polars license can be found in the licensing folder.

use polars::polars_compute;
use polars_core::prelude::*;
use polars_core::utils::polars_arrow::array::{Array, BooleanArray, PrimitiveArray, Utf8ViewArray};
use polars_core::utils::polars_arrow::types::NativeType;
use representation::rdf_to_polars::default_decimal_scale;
use representation::BaseRDFNodeType;

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

/// What the term serializers write: N-Triples terms (`<iri>`, `_:b`, `"lex"`, `@lang`), or the
/// JSON strings that SPARQL JSON results hold for them (`"iri"`, `"b"`, `"lex"`, `"lang"`).
/// Numbers, booleans and the other lexical forms are the same quoted strings in both.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum Style {
    NTriples,
    Json,
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

fn integer_serializer<I: NativeType + itoa::Integer>(
    array: &PrimitiveArray<I>,
) -> impl Serializer<'_> {
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
) -> impl Serializer<'_> {
    let f = move |&item, buf: &mut Vec<u8>| {
        let mut buffer = ryu::Buffer::new();
        // ryu writes infinity as inf, the xsd:double and xsd:float lexical form is INF
        let value = match buffer.format(item) {
            "inf" => "INF",
            "-inf" => "-INF",
            v => v,
        };
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

fn decimal_serializer(array: &PrimitiveArray<i128>, scale: usize) -> impl Serializer<'_> {
    let mut fmt_buf = polars_compute::decimal::DecimalFmtBuffer::new();
    let f = move |&item, buf: &mut Vec<u8>| {
        buf.extend_from_slice(fmt_buf.format_dec128(item, scale, true, false).as_bytes());
    };

    make_serializer::<_, _>(f, array.iter(), |array| {
        array
            .as_any()
            .downcast_ref::<PrimitiveArray<i128>>()
            .expect(ARRAY_MISMATCH_MSG)
            .iter()
    })
}

fn bool_serializer(array: &BooleanArray) -> impl Serializer<'_> {
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

/// Writes a string escaped for an N-Triples string literal and for a JSON string alike: quotes,
/// backslashes and all control characters (as `\n`, `\t`, `\r`, `\b`, `\f` or `\u00XX`, which both
/// formats read). All escaped characters are ASCII, so they never occur inside a multi-byte UTF-8
/// sequence, and the bytes can be scanned with the unescaped runs copied in bulk.
pub(super) fn write_escaped(s: &str, buf: &mut Vec<u8>) {
    let bytes = s.as_bytes();
    buf.reserve(bytes.len());
    let mut start = 0;
    for (i, &b) in bytes.iter().enumerate() {
        if b >= 0x20 && b != b'"' && b != b'\\' {
            continue;
        }
        buf.extend_from_slice(&bytes[start..i]);
        match b {
            b'\n' => buf.extend_from_slice(b"\\n"),
            b'\t' => buf.extend_from_slice(b"\\t"),
            b'\r' => buf.extend_from_slice(b"\\r"),
            b'"' => buf.extend_from_slice(b"\\\""),
            b'\\' => buf.extend_from_slice(b"\\\\"),
            0x08 => buf.extend_from_slice(b"\\b"),
            0x0c => buf.extend_from_slice(b"\\f"),
            c => {
                const HEX: &[u8; 16] = b"0123456789ABCDEF";
                buf.extend_from_slice(b"\\u00");
                buf.push(HEX[(c >> 4) as usize]);
                buf.push(HEX[(c & 0xf) as usize]);
            }
        }
        start = i + 1;
    }
    buf.extend_from_slice(&bytes[start..]);
}

/// Writes the strings as they are, for triple terms, which are decoded to their N-Triples form
/// `<<( s p o )>>` (with their literals escaped) before they are written.
pub(super) fn raw_string_serializer<'a, Iter: Send + 'a>(
    mut f: impl FnMut(&mut Iter) -> Option<&str> + Send + 'a,
    mut update: impl FnMut(&'a dyn Array) -> Iter + Send + 'a,
    array: &'a dyn Array,
) -> Box<dyn Serializer<'a> + 'a + Send> {
    struct RawSerializer<F, Iter, Update> {
        serialize: F,
        update: Update,
        iter: Iter,
    }

    impl<'a, F, Iter, Update> Serializer<'a> for RawSerializer<F, Iter, Update>
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
        if let Some(s) = f(iter) {
            buf.extend_from_slice(s.as_bytes());
        }
    };
    Box::new(RawSerializer {
        serialize,
        update,
        iter,
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

    let iter = update(array);
    let serialize = move |iter: &mut Iter, buf: &mut Vec<u8>| {
        buf.push(QUOTE_CHAR);
        let Some(s) = f(iter) else {
            return;
        };
        write_escaped(s, buf);
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
    const BLANK_NODE_PREFIX: [u8; 2] = *b"_:";

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
    style: Style,
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
        // In JSON, IRIs, blank node labels and language tags are plain (escaped) strings.
        (DataType::String, _) if style == Style::Json => string_serializer(
            |iter| Iterator::next(iter).expect(TOO_MANY_MSG),
            |arr| {
                arr.as_any()
                    .downcast_ref::<Utf8ViewArray>()
                    .expect(ARRAY_MISMATCH_MSG)
                    .iter()
            },
            array,
        ),
        (DataType::String, BaseRDFNodeType::TripleTerm) => raw_string_serializer(
            |iter| Iterator::next(iter).expect(TOO_MANY_MSG),
            |arr| {
                arr.as_any()
                    .downcast_ref::<Utf8ViewArray>()
                    .expect(ARRAY_MISMATCH_MSG)
                    .iter()
            },
            array,
        ),
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
        (DataType::String, BaseRDFNodeType::Literal(_, _)) if is_lang_tag => lang_serializer(
            |iter| Iterator::next(iter).expect(TOO_MANY_MSG),
            |arr| {
                arr.as_any()
                    .downcast_ref::<Utf8ViewArray>()
                    .expect(ARRAY_MISMATCH_MSG)
                    .iter()
            },
            array,
        ),
        (DataType::String, BaseRDFNodeType::Literal(_, _)) => string_serializer(
            |iter| Iterator::next(iter).expect(TOO_MANY_MSG),
            |arr| {
                arr.as_any()
                    .downcast_ref::<Utf8ViewArray>()
                    .expect(ARRAY_MISMATCH_MSG)
                    .iter()
            },
            array,
        ),
        (DataType::Decimal(..), _) => {
            with_quoted_serializer!(decimal_serializer, default_decimal_scale())
        }
        _ => {
            panic!("datatype {dtype} cannot be written to triples")
        }
    };
    Ok(serializer)
}
