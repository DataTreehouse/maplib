use polars::error::PolarsError;

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
        return Err(PolarsError::InvalidOperation(
            "Non UUID arg as namespace argument to struuid".into(),
        ));
    };
    Ok(use_uuid)
}
