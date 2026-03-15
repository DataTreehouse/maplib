use polars::prelude::{col, LazyFrame};
use representation::cats::{
    maybe_decode_expr, optional_maybe_decode_expr, CatReEnc, Cats, LockedCats,
};
use representation::solution_mapping::BaseCatState;
use representation::{BaseRDFNodeType, RDFNodeState};

pub enum CatOperation {
    Decode,
    ReEnc(CatReEnc),
}

impl CatOperation {
    pub fn apply(
        self,
        mut mappings: LazyFrame,
        c: &str,
        t: &RDFNodeState,
        base_t: &BaseRDFNodeType,
        global_cats: LockedCats,
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
            CatOperation::ReEnc(cat_re_enc) => {
                mappings = cat_re_enc.re_encode(mappings, c, t.is_multi(), base_t.clone(), false);
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
        BaseCatState::CategoricalNative(None) => match right {
            BaseCatState::CategoricalNative(None) => {
                (BaseCatState::CategoricalNative(None), None, None)
            }
            BaseCatState::CategoricalNative(right_local_cats) => (
                BaseCatState::CategoricalNative(right_local_cats.as_ref().cloned()),
                None,
                None,
            ),
            BaseCatState::String => (BaseCatState::String, Some(CatOperation::Decode), None),
            BaseCatState::NonString => {
                unreachable!("Should never happen")
            }
        },
        BaseCatState::CategoricalNative(Some(left_local_cats)) => match right {
            BaseCatState::CategoricalNative(None) => (
                BaseCatState::CategoricalNative(Some(left_local_cats.clone())),
                None,
                None,
            ),
            BaseCatState::CategoricalNative(Some(right_local_cats)) => {
                let re_enc = Cats::join(left_local_cats.clone(), right_local_cats.clone());
                (
                    BaseCatState::CategoricalNative(Some(left_local_cats.clone())),
                    None,
                    Some(CatOperation::ReEnc(re_enc)),
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
