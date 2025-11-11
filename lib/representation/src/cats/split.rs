use oxrdf::NamedNode;
use polars::prelude::Series;
use std::collections::HashMap;

pub fn named_node_split_prefix(nn: &NamedNode) -> NamedNode {
    NamedNode::new_unchecked(rdf_split_iri_str(nn.as_str()).0)
}

pub fn named_node_split_suffix(nn: &NamedNode) -> NamedNode {
    NamedNode::new_unchecked(rdf_split_iri_str(nn.as_str()).1)
}

pub fn split_iri_series<'a>(
    series: &'a Series,
) -> (Vec<Option<u32>>, Vec<Option<&'a str>>, HashMap<String, u32>) {
    let series_str = series.str().unwrap();
    let mut prefix_map = HashMap::new();
    let mut prefixes = Vec::with_capacity(series.len());

    let (new_prefixes, suffixes): (Vec<_>, Vec<_>) = series_str
        .iter()
        .map(|x| {
            if let Some(iri) = x {
                let (pre, suf) = rdf_split_iri_str(iri);
                (Some(pre), Some(suf))
            } else {
                (None, None)
            }
        })
        .unzip();
    for p in new_prefixes {
        if let Some(p) = p {
            if let Some(v) = prefix_map.get(p) {
                prefixes.push(Some(*v));
            } else {
                let new_v = prefix_map.len() as u32;
                prefix_map.insert(p.to_string(), new_v);
                prefixes.push(Some(new_v));
            }
        } else {
            prefixes.push(None)
        }
    }

    (prefixes, suffixes, prefix_map)
}

pub fn rdf_split_iri_str(iri: &str) -> (&str, &str) {
    return ("", iri);
    // Apache 2 / MIT The Rust Project Contributors
    #[inline]
    fn rsplit_once_inclusive_l<P: std::str::pattern::Pattern>(
        this: &str,
        delimiter: P,
    ) -> Option<(&'_ str, &'_ str)>
    where
        for<'a> P::Searcher<'a>: std::str::pattern::ReverseSearcher<'a>,
    {
        let (_, end) = std::str::pattern::ReverseSearcher::next_match_back(
            &mut delimiter.into_searcher(this),
        )?;
        // SAFETY: `Searcher` is known to return valid indices.
        unsafe { Some((this.get_unchecked(..end), this.get_unchecked(end..))) }
    }

    const DELIMITERS: &[char] = &['/', '#', ':'];

    let (prefix, suffix) = match rsplit_once_inclusive_l(iri, DELIMITERS) {
        Some(pair) => pair,
        None => ("", iri),
    };
    (prefix, suffix)
}
