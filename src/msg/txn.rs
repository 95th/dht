use ben::{Encode, Encoder};

#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub struct TxnId(pub u16);

impl TxnId {
    pub fn next_id(&mut self) -> Self {
        let out = *self;
        self.0 = self.0.wrapping_add(1);
        out
    }
}

impl Encode for TxnId {
    fn encode<E: Encoder>(&self, enc: &mut E) {
        enc.add_bytes(&self.0.to_be_bytes()[..]);
    }
}
