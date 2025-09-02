use polars::prelude::{CategoricalMapping, CategoricalPhysical, Categories, PlSmallStr};
use std::sync::Arc;

pub fn global_polars_cat() -> Arc<Categories> {
    Arc::from(Categories::new(
        PlSmallStr::from_str("cats"),
        PlSmallStr::from_str("global"),
        CategoricalPhysical::U32,
    ))
}

pub fn global_polars_cat_mapping() -> Arc<CategoricalMapping> {
    Arc::from(CategoricalMapping::new(usize::MAX))
}
