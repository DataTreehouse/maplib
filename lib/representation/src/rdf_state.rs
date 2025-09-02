use crate::solution_mapping::BaseCatState;
use crate::{polars_lang_fields, BaseRDFNodeType};
use oxrdf::vocab::{rdf, xsd};
use oxrdf::NamedNodeRef;
use polars::datatypes::{DataType, Field, PlSmallStr};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};

#[derive(Clone, Debug)]
pub struct RDFNodeState {
    pub map: HashMap<BaseRDFNodeType, BaseCatState>,
}

impl RDFNodeState {
    pub fn from_bases(t: BaseRDFNodeType, s: BaseCatState) -> RDFNodeState {
        RDFNodeState {
            map: HashMap::from([(t, s)]),
        }
    }

    pub fn from_map(map: HashMap<BaseRDFNodeType, BaseCatState>) -> RDFNodeState {
        RDFNodeState { map }
    }

    pub fn default_from_types(types: Vec<BaseRDFNodeType>) -> RDFNodeState {
        let mut map = HashMap::new();
        for t in types {
            let state = t.default_input_cat_state();
            map.insert(t, state);
        }
        RDFNodeState { map }
    }

    pub fn is_multi(&self) -> bool {
        self.map.len() > 1
    }

    pub fn is_literal(&self) -> bool {
        if let Some(b) = self.get_base_type() {
            matches!(b, BaseRDFNodeType::Literal(_))
        } else {
            false
        }
    }

    pub fn polars_data_type(&self) -> DataType {
        if self.is_multi() {
            let mut fields = Vec::new();
            for (t, s) in &self.map {
                if t.is_lang_string() {
                    fields.extend(polars_lang_fields(s))
                } else {
                    let n = t.field_col_name();
                    fields.push(Field::new(
                        PlSmallStr::from_str(&n),
                        t.polars_data_type(s, false),
                    ));
                }
            }
            DataType::Struct(fields)
        } else {
            let base_state = self.get_base_state();
            self.get_base_type()
                .unwrap()
                .polars_data_type(base_state.unwrap(), false)
        }
    }

    pub fn get_base_type(&self) -> Option<&BaseRDFNodeType> {
        if !self.is_multi() {
            self.map.keys().next()
        } else {
            None
        }
    }

    pub fn get_base_state(&self) -> Option<&BaseCatState> {
        if !self.is_multi() {
            self.map.values().next()
        } else {
            None
        }
    }

    pub fn get_sorted_types(&self) -> Vec<&BaseRDFNodeType> {
        let mut types: Vec<_> = self.map.keys().collect();
        types.sort();
        types
    }

    pub fn types_equal(&self, other: &RDFNodeState) -> bool {
        if self.map.len() != other.map.len() {
            false
        } else {
            for k in self.map.keys() {
                if !other.map.contains_key(k) {
                    return false;
                }
            }
            true
        }
    }

    // pub fn infer_from_term_pattern(tp: &TermPattern) -> Option<RDFNodeType> {
    //     match tp {
    //         TermPattern::NamedNode(nn) => Some(RDFNodeType::IRI(Some(named_node_split_prefix(nn)))),
    //         TermPattern::BlankNode(_) => None,
    //         TermPattern::Literal(l) => Some(RDFNodeType::Literal(l.datatype().into_owned())),
    //         TermPattern::Variable(_v) => None,
    //         #[cfg(feature = "rdf-star")]
    //         TermPattern::Triple(_) => todo!(),
    //     }
    // }

    pub fn is_iri(&self) -> bool {
        if let Some(b) = self.get_base_type() {
            b.is_iri()
        } else {
            false
        }
    }
    pub fn is_blank_node(&self) -> bool {
        if let Some(b) = self.get_base_type() {
            b.is_blank_node()
        } else {
            false
        }
    }

    pub fn is_none(&self) -> bool {
        if let Some(b) = self.get_base_type() {
            b.is_none()
        } else {
            false
        }
    }

    pub fn is_lang_string(&self) -> bool {
        self.is_lit_type(rdf::LANG_STRING)
    }

    pub fn is_lit_type(&self, nnref: NamedNodeRef) -> bool {
        if let Some(b) = self.get_base_type() {
            b.is_lit_type(nnref)
        } else {
            false
        }
    }

    pub fn is_bool(&self) -> bool {
        self.is_lit_type(xsd::BOOLEAN)
    }

    pub fn is_float(&self) -> bool {
        self.is_lit_type(xsd::FLOAT)
    }

    pub fn is_numeric(&self) -> bool {
        if let Some(b) = self.get_base_type() {
            b.is_numeric()
        } else {
            false
        }
    }
}

impl Display for RDFNodeState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.is_multi() {
            let type_strings: Vec<_> = self.map.iter().map(|(x, _)| x.to_string()).collect();
            write!(f, "Multiple({})", type_strings.join(", "))
        } else {
            self.get_base_type().unwrap().fmt(f)
        }
    }
}
