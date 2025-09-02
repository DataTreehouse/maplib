use polars::prelude::{by_name, col, LazyFrame};
use representation::cats::{maybe_decode_expr, optional_maybe_decode_expr, CatReEnc, Cats};
use representation::solution_mapping::BaseCatState;
use representation::{BaseRDFNodeType, RDFNodeState};
use std::sync::Arc;

pub enum CatOperation {
    Decode,
    ReEnc(CatReEnc, bool),
}

impl CatOperation {
    pub fn apply(
        self,
        mut mappings: LazyFrame,
        c: &str,
        t: &RDFNodeState,
        base_t: &BaseRDFNodeType,
        global_cats: Arc<Cats>,
    ) -> LazyFrame {
        match self {
            CatOperation::Decode => {
                if t.is_multi() || t.is_lang_string() {
                    let mut fields = vec![];
                    for s in base_t.multi_columns() {
                        if let Some(e) = optional_maybe_decode_expr(
                            col(c).struct_().field_by_name(&s),
                            base_t,
                            t.map.get(base_t).unwrap(),
                            global_cats.clone(),
                        ) {
                            fields.push(e);
                        }
                    }
                    if !fields.is_empty() {
                        mappings = mappings.with_column(col(c).struct_().with_fields(fields));
                    }
                } else {
                    mappings = mappings.with_column(maybe_decode_expr(
                        col(c),
                        base_t,
                        t.map.get(base_t).unwrap(),
                        global_cats.clone(),
                    ));
                }
            }
            CatOperation::ReEnc(cat_re_enc, forget_others) => {
                if !t.is_multi() {
                    mappings = cat_re_enc.re_encode(mappings, c, forget_others);
                } else {
                    let tmp = uuid::Uuid::new_v4().to_string();
                    let n = base_t.field_col_name();
                    mappings = mappings.with_column(
                        col(c)
                            .struct_()
                            .field_by_name(&n)
                            .alias(&tmp),
                    );
                    mappings = cat_re_enc.re_encode(mappings, &tmp, forget_others);
                    mappings = mappings.with_column(col(c).struct_().with_fields(vec![col(&tmp).alias(&n)]));
                    mappings = mappings.drop(by_name([tmp], true));
                }
            }
        }
        mappings
    }
}

pub fn create_compatible_cats(
    left: &BaseCatState,
    right: &BaseCatState,
) -> (BaseCatState, Option<CatOperation>, Option<CatOperation>) {
    match left {
        BaseCatState::CategoricalNative(_left_sorted, None) => match right {
            BaseCatState::CategoricalNative(_right_sorted, None) => {
                //TODO! FIX SO THAT WE CAN SET TO && BECAUSE DEPENDS ON GLOBAL SORT STATE
                (BaseCatState::CategoricalNative(false, None), None, None)
            }
            BaseCatState::CategoricalNative(_, right_local_cats) => (
                BaseCatState::CategoricalNative(false, right_local_cats.as_ref().cloned()),
                None,
                None,
            ),
            BaseCatState::String => (BaseCatState::String, Some(CatOperation::Decode), None),
            BaseCatState::NonString => {
                unreachable!("Should never happen")
            }
        },
        BaseCatState::CategoricalNative(_, Some(left_local_cats)) => match right {
            BaseCatState::CategoricalNative(_, None) => (
                BaseCatState::CategoricalNative(false, Some(left_local_cats.clone())),
                None,
                None,
            ),
            BaseCatState::CategoricalNative(_, Some(right_local_cats)) => {
                let re_enc = Cats::join(left_local_cats.clone(), right_local_cats.clone());
                (
                    BaseCatState::CategoricalNative(false, Some(left_local_cats.clone())),
                    None,
                    Some(CatOperation::ReEnc(re_enc, true)),
                )
            }
            BaseCatState::String | BaseCatState::NonString => {
                unreachable!("Should never happen")
            }
        },
        BaseCatState::String => match right {
            BaseCatState::CategoricalNative(..) => {
                (BaseCatState::String, None, Some(CatOperation::Decode))
            }
            BaseCatState::NonString => {
                unreachable!("Should never happen")
            }
            BaseCatState::String => (BaseCatState::String, None, None),
        },
        BaseCatState::NonString => match right {
            BaseCatState::CategoricalNative(..) | BaseCatState::String => {
                unreachable!("Should never happen")
            }
            BaseCatState::NonString => (BaseCatState::NonString, None, None),
        },
    }
}
