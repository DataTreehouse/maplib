use oxrdf::vocab::xsd;
use pyo3::{pyclass, pymethods};
use representation::python::PyIRI;

#[derive(Clone, Debug)]
#[pyclass(name = "xsd")]
pub struct PyXSD {}

#[pymethods]
impl PyXSD {
    #[classattr]
    fn boolean() -> PyIRI {
        PyIRI::new(xsd::BOOLEAN.as_str().to_string()).unwrap()
    }

    #[classattr]
    fn byte() -> PyIRI {
        PyIRI::new(xsd::BYTE.as_str().to_string()).unwrap()
    }

    #[classattr]
    fn date() -> PyIRI {
        PyIRI::new(xsd::DATE.as_str().to_string()).unwrap()
    }

    #[classattr]
    #[pyo3(name = "dateTime")]
    fn date_time() -> PyIRI {
        PyIRI::new(xsd::DATE_TIME.as_str().to_string()).unwrap()
    }

    #[classattr]
    #[pyo3(name = "dateTimeStamp")]
    fn date_time_stamp() -> PyIRI {
        PyIRI::new(xsd::DATE_TIME_STAMP.as_str().to_string()).unwrap()
    }

    #[classattr]
    fn decimal() -> PyIRI {
        PyIRI::new(xsd::DECIMAL.as_str().to_string()).unwrap()
    }

    #[classattr]
    fn double() -> PyIRI {
        PyIRI::new(xsd::DOUBLE.as_str().to_string()).unwrap()
    }

    #[classattr]
    fn duration() -> PyIRI {
        PyIRI::new(xsd::DURATION.as_str().to_string()).unwrap()
    }

    #[classattr]
    fn float() -> PyIRI {
        PyIRI::new(xsd::FLOAT.as_str().to_string()).unwrap()
    }

    #[classattr]
    fn int_() -> PyIRI {
        PyIRI::new(xsd::INT.as_str().to_string()).unwrap()
    }

    #[classattr]
    fn integer() -> PyIRI {
        PyIRI::new(xsd::INTEGER.as_str().to_string()).unwrap()
    }

    #[classattr]
    fn language() -> PyIRI {
        PyIRI::new(xsd::LANGUAGE.as_str().to_string()).unwrap()
    }

    #[classattr]
    fn long() -> PyIRI {
        PyIRI::new(xsd::LONG.as_str().to_string()).unwrap()
    }

    #[classattr]
    fn short() -> PyIRI {
        PyIRI::new(xsd::SHORT.as_str().to_string()).unwrap()
    }

    #[classattr]
    fn string() -> PyIRI {
        PyIRI::new(xsd::STRING.as_str().to_string()).unwrap()
    }

    #[classattr]
    #[pyo3(name = "anyURI")]
    fn any_uri() -> PyIRI {
        PyIRI::new(xsd::ANY_URI.as_str().to_string()).unwrap()
    }

    #[classattr]
    #[pyo3(name = "dayTimeDuration")]
    fn day_time_duration() -> PyIRI {
        PyIRI::new(xsd::DAY_TIME_DURATION.as_str().to_string()).unwrap()
    }

    #[classattr]
    #[pyo3(name = "base64Binary")]
    fn base_64_binary() -> PyIRI {
        PyIRI::new(xsd::BASE_64_BINARY.as_str().to_string()).unwrap()
    }

    #[classattr]
    #[pyo3(name = "gDay")]
    fn g_day() -> PyIRI {
        PyIRI::new(xsd::G_DAY.as_str().to_string()).unwrap()
    }

    #[classattr]
    #[pyo3(name = "gMonthDay")]
    fn g_month_day() -> PyIRI {
        PyIRI::new(xsd::G_MONTH_DAY.as_str().to_string()).unwrap()
    }

    #[classattr]
    #[pyo3(name = "gMonth")]
    fn g_month() -> PyIRI {
        PyIRI::new(xsd::G_MONTH.as_str().to_string()).unwrap()
    }

    #[classattr]
    #[pyo3(name = "gYear")]
    fn g_year() -> PyIRI {
        PyIRI::new(xsd::G_YEAR.as_str().to_string()).unwrap()
    }

    #[classattr]
    #[pyo3(name = "gYearMonth")]
    fn g_year_month() -> PyIRI {
        PyIRI::new(xsd::G_YEAR_MONTH.as_str().to_string()).unwrap()
    }

    #[classattr]
    #[pyo3(name = "hexBinary")]
    fn hex_binary() -> PyIRI {
        PyIRI::new(xsd::HEX_BINARY.as_str().to_string()).unwrap()
    }

    #[classattr]
    #[pyo3(name = "Name")]
    fn name() -> PyIRI {
        PyIRI::new(xsd::NAME.as_str().to_string()).unwrap()
    }

    #[classattr]
    #[pyo3(name = "NCName")]
    fn nc_name() -> PyIRI {
        PyIRI::new(xsd::NC_NAME.as_str().to_string()).unwrap()
    }

    #[classattr]
    #[pyo3(name = "NMTOKEN")]
    fn nmtoken() -> PyIRI {
        PyIRI::new(xsd::NMTOKEN.as_str().to_string()).unwrap()
    }

    #[classattr]
    #[pyo3(name = "negativeInteger")]
    fn negative_integer() -> PyIRI {
        PyIRI::new(xsd::NEGATIVE_INTEGER.as_str().to_string()).unwrap()
    }

    #[classattr]
    #[pyo3(name = "nonNegativeInteger")]
    fn non_negative_integer() -> PyIRI {
        PyIRI::new(xsd::NON_NEGATIVE_INTEGER.as_str().to_string()).unwrap()
    }

    #[classattr]
    #[pyo3(name = "nonPositiveInteger")]
    fn non_positive_integer() -> PyIRI {
        PyIRI::new(xsd::NON_POSITIVE_INTEGER.as_str().to_string()).unwrap()
    }

    #[classattr]
    #[pyo3(name = "normalizedString")]
    fn normalized_string() -> PyIRI {
        PyIRI::new(xsd::NORMALIZED_STRING.as_str().to_string()).unwrap()
    }

    #[classattr]
    #[pyo3(name = "token")]
    fn token() -> PyIRI {
        PyIRI::new(xsd::TOKEN.as_str().to_string()).unwrap()
    }

    #[classattr]
    #[pyo3(name = "unsignedByte")]
    fn unsigned_byte() -> PyIRI {
        PyIRI::new(xsd::UNSIGNED_BYTE.as_str().to_string()).unwrap()
    }

    #[classattr]
    #[pyo3(name = "unsignedInt")]
    fn unsigned_int() -> PyIRI {
        PyIRI::new(xsd::UNSIGNED_INT.as_str().to_string()).unwrap()
    }

    #[classattr]
    #[pyo3(name = "unsignedLong")]
    fn unsigned_long() -> PyIRI {
        PyIRI::new(xsd::UNSIGNED_LONG.as_str().to_string()).unwrap()
    }

    #[classattr]
    #[pyo3(name = "unsignedShort")]
    fn unsigned_short() -> PyIRI {
        PyIRI::new(xsd::UNSIGNED_SHORT.as_str().to_string()).unwrap()
    }

    #[classattr]
    #[pyo3(name = "yearMonthDuration")]
    fn year_month_duration() -> PyIRI {
        PyIRI::new(xsd::YEAR_MONTH_DURATION.as_str().to_string()).unwrap()
    }
}
