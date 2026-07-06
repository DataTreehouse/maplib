use crate::column_builder::SolutionMappingsColumnBuilder;
use crate::errors::RepresentationError;
use crate::solution_mapping::EagerSolutionMappings;
use polars_core::frame::DataFrame;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use sparesults::QuerySolution;
use std::collections::HashMap;

pub struct SolutionMappingsBuilder {
    map: HashMap<String, SolutionMappingsColumnBuilder>,
    capacity: usize,
    height: usize,
}

impl SolutionMappingsBuilder {
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

    pub fn extend_query_solutions(
        &mut self,
        sols: &[QuerySolution],
    ) -> Result<(), RepresentationError> {
        if sols.is_empty() {
            return Ok(());
        }
        let vars = sols.first().unwrap().variables();
        for v in vars {
            if !self.map.contains_key(v.as_str()) {
                let mut builder = SolutionMappingsColumnBuilder::new_with_capacity(self.capacity);
                if self.height > 0 {
                    for _ in 0..self.height {
                        builder.push_none()
                    }
                }
                self.map.insert(v.as_str().to_owned(), builder);
            }
        }
        for sol in sols {
            for (v, s) in sol.variables().iter().zip(sol.values()) {
                let builder = self.map.get_mut(v.as_str()).unwrap();
                builder.push_maybe_term(s)?;
            }
        }
        Ok(())
    }

    pub fn finish(self) -> Result<EagerSolutionMappings, RepresentationError> {
        let cols_states: Result<Vec<_>, RepresentationError> = self
            .map
            .into_par_iter()
            .map(|(x, y)| {
                let (c, s) = y.finish(&x)?;
                Ok((c, x, s))
            })
            .collect();
        let cols_states = cols_states?;
        let mut height = 0;
        let mut columns = Vec::with_capacity(cols_states.len());
        let mut states = HashMap::with_capacity(cols_states.len());
        for (c, x, s) in cols_states {
            height = c.len();
            columns.push(c);
            states.insert(x, s);
        }
        let df = DataFrame::new(height, columns).unwrap();
        let sm = EagerSolutionMappings::new(df, states);
        Ok(sm)
    }
}
