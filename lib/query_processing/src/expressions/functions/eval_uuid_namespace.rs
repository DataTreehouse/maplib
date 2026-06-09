use polars::error::PolarsError;
use std::str::FromStr;

pub fn eval_uuid_namespace(arg1: &str) -> Result<uuid::Uuid, PolarsError> {
    let use_uuid = if arg1 == "dns" {
        uuid::Uuid::NAMESPACE_DNS
    } else if arg1 == "oid" {
        uuid::Uuid::NAMESPACE_OID
    } else if arg1 == "url" {
        uuid::Uuid::NAMESPACE_URL
    } else if arg1 == "x500" {
        uuid::Uuid::NAMESPACE_X500
    } else {
        uuid::Uuid::from_str(arg1).map_err(|_|PolarsError::InvalidOperation(
            format!("Namespace argument must either be a uuid string or one of the predefined namespaces (dns,oid,url and x500) {}", arg1).into(),
        ))?
    };
    Ok(use_uuid)
}
