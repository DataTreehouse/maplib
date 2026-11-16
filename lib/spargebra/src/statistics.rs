//! The statistical aggregates of Apache Jena ARQ, in the namespace
//! `http://jena.apache.org/ARQ/function/aggregate#` (`agg:`). As in Jena, they are also
//! registered in the ARQ function namespace `http://jena.apache.org/ARQ/function#` (`afn:`).
//!
//! See <https://jena.apache.org/documentation/query/library-function.html>.

use oxrdf::NamedNode;

/// The Apache Jena ARQ aggregate function namespace.
pub const JENA_AGGREGATE_NAMESPACE: &str = "http://jena.apache.org/ARQ/function/aggregate#";
/// The Apache Jena ARQ function namespace, where Jena registers the same aggregates again.
pub const JENA_FUNCTION_NAMESPACE: &str = "http://jena.apache.org/ARQ/function#";

/// A statistical aggregate. All of them return an `xsd:double`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum StatisticalAggregate {
    /// `agg:stdev` and `agg:stdev_samp`, the sample standard deviation.
    StdevSample,
    /// `agg:stdev_pop`, the population standard deviation.
    StdevPopulation,
    /// `agg:variance` and `agg:var_samp`, the sample variance.
    VarianceSample,
    /// `agg:var_pop`, the population variance.
    VariancePopulation,
}

/// The local names of the aggregates, as in Jena's `StandardAggregates`.
pub const STATISTICAL_AGGREGATE_NAMES: [(&str, StatisticalAggregate); 6] = [
    ("stdev", StatisticalAggregate::StdevSample),
    ("stdev_samp", StatisticalAggregate::StdevSample),
    ("stdev_pop", StatisticalAggregate::StdevPopulation),
    ("variance", StatisticalAggregate::VarianceSample),
    ("var_samp", StatisticalAggregate::VarianceSample),
    ("var_pop", StatisticalAggregate::VariancePopulation),
];

impl StatisticalAggregate {
    /// The aggregate named by an IRI in the `agg:` or `afn:` namespace.
    pub fn from_iri(iri: &str) -> Option<Self> {
        let local = iri
            .strip_prefix(JENA_AGGREGATE_NAMESPACE)
            .or_else(|| iri.strip_prefix(JENA_FUNCTION_NAMESPACE))?;
        STATISTICAL_AGGREGATE_NAMES
            .iter()
            .find(|(name, _)| *name == local)
            .map(|(_, a)| *a)
    }

    /// The delta degrees of freedom: 1 for the sample statistics, 0 for the population ones.
    pub fn ddof(&self) -> u8 {
        match self {
            StatisticalAggregate::StdevSample | StatisticalAggregate::VarianceSample => 1,
            StatisticalAggregate::StdevPopulation | StatisticalAggregate::VariancePopulation => 0,
        }
    }

    pub fn is_stdev(&self) -> bool {
        matches!(
            self,
            StatisticalAggregate::StdevSample | StatisticalAggregate::StdevPopulation
        )
    }
}

/// All IRIs of the statistical aggregates, in both namespaces.
pub fn statistical_aggregate_iris() -> impl Iterator<Item = NamedNode> {
    [JENA_AGGREGATE_NAMESPACE, JENA_FUNCTION_NAMESPACE]
        .into_iter()
        .flat_map(|ns| {
            STATISTICAL_AGGREGATE_NAMES
                .iter()
                .map(move |(name, _)| NamedNode::new_unchecked(format!("{ns}{name}")))
        })
}
