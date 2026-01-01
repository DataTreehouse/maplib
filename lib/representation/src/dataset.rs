use oxrdf::{BlankNode, GraphName, NamedNode};
use spargebra::algebra::QueryDataset;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub enum NamedGraph {
    DefaultGraph,
    NamedGraph(NamedNode),
    BlankNode(BlankNode),
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

impl From<&GraphName> for NamedGraph {
    fn from(graph: &GraphName) -> Self {
        match graph {
            GraphName::NamedNode(nn) => NamedGraph::NamedGraph(nn.clone()),
            GraphName::BlankNode(bn) => NamedGraph::BlankNode(bn.clone()),
            GraphName::DefaultGraph => NamedGraph::DefaultGraph,
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
            NamedGraph::BlankNode(bn) => {
                write!(f, "{}", bn)
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
