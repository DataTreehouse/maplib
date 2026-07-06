use crate::errors::RepresentationError;
use crate::{
    BaseRDFNodeType, BaseRDFNodeTypeRef, RDFNodeState, SeriesBuilder, LANG_STRING_LANG_FIELD,
    LANG_STRING_VALUE_FIELD,
};
use oxrdf::Term;
use polars::prelude::{as_struct, col, IntoLazy};
use polars_core::frame::DataFrame;
use polars_core::prelude::{Column, IntoColumn, Series};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use std::collections::HashMap;

pub struct SolutionMappingsColumnBuilder {
    map: HashMap<String, SeriesBuilder>,
    capacity: usize,
    height: usize,
}

impl SolutionMappingsColumnBuilder {
    pub fn new() -> Self {
        Self::new_with_capacity(0)
    }

    pub fn new_with_capacity(capacity: usize) -> Self {
        Self {
            map: Default::default(),
            capacity,
            height: 0,
        }
    }

    pub fn push_maybe_term(
        &mut self,
        maybe_term: &Option<Term>,
    ) -> Result<(), RepresentationError> {
        match maybe_term {
            None => {
                self.push_none();
                Ok(())
            }
            Some(term) => self.push_term(term),
        }
    }

    pub fn push_term(&mut self, term: &Term) -> Result<(), RepresentationError> {
        let t_ref = BaseRDFNodeTypeRef::from_term(term);
        self.maybe_add_builder_with_height(&t_ref);
        let v = self.map.get_mut(t_ref.as_str()).unwrap();
        v.parse_term(term)?;
        self.push_others_none(&t_ref);
        self.height += 1;
        Ok(())
    }

    pub fn push_none(&mut self) {
        let t_ref = BaseRDFNodeTypeRef::None;
        self.maybe_add_builder_with_height(&t_ref);
        self.map.get_mut(t_ref.as_str()).unwrap().push_none();
        self.push_others_none(&t_ref);
    }

    fn maybe_add_builder_with_height(&mut self, t_ref: &BaseRDFNodeTypeRef) {
        if !self.map.contains_key(t_ref.as_str()) {
            let t = t_ref.clone().into_owned();
            let mut builder = SeriesBuilder::new_with_capacity(&t, self.capacity);
            for _ in 0..self.height {
                builder.push_none();
            }
            self.map.insert(t_ref.as_str().to_string(), builder);
        }
    }

    fn push_others_none(&mut self, t_ref: &BaseRDFNodeTypeRef) {
        for (t, b) in self.map.iter_mut() {
            if t != t_ref.as_str() {
                b.push_none();
            }
        }
    }

    pub fn finish(self, name: &str) -> Result<(Column, RDFNodeState), RepresentationError> {
        let mut series_types: Vec<_> = self
            .map
            .into_par_iter()
            .map(|(k, v)| {
                let t = BaseRDFNodeType::from_string(k);
                let ser = v.into_series(name);
                (ser, t)
            })
            .collect();
        Ok(if series_types.is_empty() {
            let ser = Series::new_empty(
                name.into(),
                &BaseRDFNodeType::None.default_input_polars_data_type(),
            );
            (
                ser.into_column(),
                BaseRDFNodeType::None.into_default_input_rdf_node_state(),
            )
        } else if series_types.len() > 0 {
            let mut columns = Vec::with_capacity(series_types.len());
            let mut states = HashMap::new();
            let mut height = 0;
            let mut struct_exprs = vec![];
            for (mut ser, t) in series_types {
                height = ser.len();
                if t.is_lang_string() {
                    let mut v_col = ser
                        .struct_()
                        .unwrap()
                        .field_by_name(LANG_STRING_VALUE_FIELD)
                        .unwrap()
                        .into_column();
                    v_col.rename(LANG_STRING_VALUE_FIELD.into());
                    let mut l_col = ser
                        .struct_()
                        .unwrap()
                        .field_by_name(LANG_STRING_LANG_FIELD)
                        .unwrap()
                        .into_column();
                    l_col.rename(LANG_STRING_LANG_FIELD.into());
                    columns.push(v_col);
                    columns.push(l_col);
                    struct_exprs.push(col(LANG_STRING_VALUE_FIELD));
                    struct_exprs.push(col(LANG_STRING_LANG_FIELD));
                } else {
                    ser.rename(t.field_col_name().as_str().into());
                    columns.push(ser.into_column());
                    struct_exprs.push(col(t.field_col_name()));
                }
                let state = t.default_input_cat_state();
                states.insert(t, state);
            }
            let mut df = DataFrame::new(height, columns).unwrap();
            df = df
                .lazy()
                .with_column(as_struct(struct_exprs).alias(name))
                .select([col(name)])
                .collect()
                .unwrap();
            let c = df.drop_in_place(name).unwrap();
            (c, RDFNodeState::from_map(states))
        } else {
            let (s, t) = series_types.pop().unwrap();
            (s.into_column(), t.into_default_input_rdf_node_state())
        })
    }
}
