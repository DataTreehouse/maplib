use oxrdf::NamedNode;
use spargebra::algebra::QueryDataset;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub enum NamedGraph {
    DefaultGraph,
    NamedGraph(NamedNode),
}

impl NamedGraph {
    pub fn from_maybe_named_node(nn: Option<&NamedNode>) -> NamedGraph {
        if let Some(nn) = nn {
            NamedGraph::NamedGraph(nn.clone())
        } else {
            NamedGraph::DefaultGraph
        }
    }
}

impl Default for NamedGraph {
    fn default() -> Self {
        NamedGraph::DefaultGraph
    }
}

impl Display for NamedGraph {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            NamedGraph::DefaultGraph => {
                write!(f, "default graph")
            }
            NamedGraph::NamedGraph(nn) => {
                write!(f, "{}", nn)
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum QueryGraph {
    NamedGraph(NamedGraph),
    QueryDataset(QueryDataset),
}

impl QueryGraph {
    pub fn from_named_graph(ng: &NamedGraph) -> QueryGraph {
        QueryGraph::NamedGraph(ng.clone())
    }
}
