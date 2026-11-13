use crate::triples::{Id, TripleId, TriplesBitmap};
use qwt::AccessUnsigned;

// see "Exchange and Consumption of Huge RDF Data" by Martinez et al. 2012
// https://link.springer.com/chapter/10.1007/978-3-642-30284-8_36
// actually only an object iterator when SPO order is used
// TODO test with other orders and fix if broken

/// Iterator over all triples with a given object ID, answering an (?S,?P,O) query.
pub struct ObjectIter<'a> {
    triples: &'a TriplesBitmap,
    o: Id,
    pos_index: usize,
    max_index: usize,
}

impl<'a> ObjectIter<'a> {
    /// Create a new iterator over all triples with the given object ID.
    /// Panics if the object does not exist.
    pub fn new(triples: &'a TriplesBitmap, o: Id) -> Self {
        assert!(o != 0, "object 0 does not exist, cant iterate");
        let pos_index = triples.op_index.find(o);
        let max_index = triples.op_index.last(o);
        //println!("ObjectIter o={} pos_index={} max_index={}", o, pos_index, max_index);
        ObjectIter { triples, o, pos_index, max_index }
    }
}

impl Iterator for ObjectIter<'_> {
    type Item = TripleId;
    fn next(&mut self) -> Option<Self::Item> {
        if self.pos_index > self.max_index {
            return None;
        }
        let pos_y = self.triples.op_index.sequence.get(self.pos_index);
        let y = self.triples.wavelet_y.get(pos_y).unwrap() as Id;
        let x = self.triples.bitmap_y.rank(pos_y) as Id + 1;
        self.pos_index += 1;
        Some([x, y, self.o])
    }
}
