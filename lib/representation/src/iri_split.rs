use crate::{IRI_PREFIX_FIELD, IRI_SUFFIX_FIELD};
use polars::frame::DataFrame;
use polars::prelude::{as_struct, col, LazyFrame};

pub fn col_not_struct(df: &DataFrame, column_name: &str) -> bool {
    // Check if the frame already has a struct for its iri
    let index = df
        .get_column_index(column_name)
        .expect("Column name is not in dataframe");
    !df.dtypes()[index].is_struct()
}

pub fn lf_split_iri(mut lf: LazyFrame, column_name: &str) -> LazyFrame {
    lf = lf.with_column(
        col(column_name)
            .str()
            .extract_groups("(.+[#/:])?(.+)$") // Regex is probably slow
            .unwrap(),
    );
    lf = lf.with_column(
        as_struct(vec![
            col(column_name)
                .struct_()
                .field_by_name("1")
                .alias(IRI_PREFIX_FIELD),
            col(column_name)
                .struct_()
                .field_by_name("2")
                .alias(IRI_SUFFIX_FIELD),
        ])
        .alias(column_name),
    );
    lf
}
