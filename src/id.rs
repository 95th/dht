use ben::{Encode, Encoder};
use data_encoding::HEXUPPER_PERMISSIVE as hex_decoder;
use rand::distributions::uniform::{SampleBorrow, SampleUniform, UniformSampler};
use rand::Rng;
use std::fmt;
use std::ops::{BitAnd, BitXor, Deref, DerefMut};

#[derive(Copy, Clone, Default, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[repr(transparent)]
pub struct NodeId(pub [u8; 20]);

impl fmt::Debug for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", &self[..])
    }
}

impl NodeId {
    pub const fn new() -> Self {
        Self::all(0)
    }

    pub const fn max() -> Self {
        Self::all(u8::max_value())
    }

    pub const fn all(b: u8) -> Self {
        Self([b; 20])
    }

    pub fn gen() -> Self {
        let mut id = Self::new();
        rand::thread_rng().fill(&mut id[..]);
        id
    }

    pub fn gen_lz(leading_zeros: usize) -> Self {
        let mask = Self::mask_lz(leading_zeros);
        let gen = Self::gen();
        gen & mask
    }

    pub fn from_hex(hex: &[u8]) -> anyhow::Result<Self> {
        let len = hex_decoder.decode_len(hex.len())?;
        anyhow::ensure!(len == 20, "Invalid hex for node ID");

        let mut id = Self::new();
        if let Err(e) = hex_decoder.decode_mut(hex, &mut id[..]) {
            anyhow::bail!("Unable to parse hex string: {:?}", e);
        }

        Ok(id)
    }

    pub fn is_zero(&self) -> bool {
        self.iter().all(|b| *b == 0)
    }

    pub fn encode_hex(&self, buf: &mut [u8]) -> anyhow::Result<()> {
        let len = hex_decoder.encode_len(self.len());
        anyhow::ensure!(len == buf.len(), "Invalid hex for node ID");

        hex_decoder.encode_mut(&self[..], buf);
        Ok(())
    }

    /// Returns number of leading zeros.
    pub fn lz(&self) -> usize {
        let mut n = 0;
        for &c in self.iter() {
            if c == 0 {
                n += 8;
            } else {
                n += c.leading_zeros() as usize;
                break;
            }
        }
        n
    }

    /// Returns number of leading zeros of `XOR` of `self` with given `NodeId`
    pub fn xlz(&self, other: &Self) -> usize {
        (self ^ other).lz()
    }

    pub fn mask_lz(leading_zeros: usize) -> Self {
        if leading_zeros >= 160 {
            return Self::new();
        }

        let mut id = Self::max();
        for i in 0..leading_zeros / 8 {
            id[i] = 0;
        }

        if leading_zeros % 8 != 0 {
            let idx = leading_zeros / 8;
            id[idx] = 0xff >> (leading_zeros % 8);
        }

        id
    }
}

impl From<[u8; 20]> for NodeId {
    fn from(buf: [u8; 20]) -> Self {
        Self(buf)
    }
}

impl Deref for NodeId {
    type Target = [u8; 20];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for NodeId {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Encode for NodeId {
    fn encode<E: Encoder>(&self, enc: &mut E) {
        enc.add_bytes(&self[..]);
    }
}

macro_rules! bit_ops {
    ($from_ty:ty, $to_ty:ty) => {
        impl BitAnd<$from_ty> for $to_ty {
            type Output = NodeId;

            fn bitand(self, other: $from_ty) -> Self::Output {
                let mut id = NodeId::new();
                id.iter_mut()
                    .zip(self.iter().zip(other.iter()))
                    .for_each(|(a, (b, c))| *a = b & c);
                id
            }
        }

        impl BitXor<$from_ty> for $to_ty {
            type Output = NodeId;

            fn bitxor(self, other: $from_ty) -> NodeId {
                let mut id = NodeId::new();
                id.iter_mut()
                    .zip(self.iter().zip(other.iter()))
                    .for_each(|(a, (b, c))| *a = b ^ c);
                id
            }
        }
    };
}

bit_ops!(&NodeId, &NodeId);
bit_ops!(&NodeId, NodeId);
bit_ops!(NodeId, &NodeId);
bit_ops!(NodeId, NodeId);

impl SampleUniform for NodeId {
    type Sampler = UniformNodeId;
}

pub struct UniformNodeId {
    low: NodeId,
    high: NodeId,
    inclusive: bool,
}

impl UniformSampler for UniformNodeId {
    type X = NodeId;

    fn new<B1, B2>(low: B1, high: B2) -> Self
    where
        B1: SampleBorrow<Self::X> + Sized,
        B2: SampleBorrow<Self::X> + Sized,
    {
        let low = low.borrow();
        let high = high.borrow();
        assert!(low < high);

        UniformNodeId {
            low: *low,
            high: *high,
            inclusive: false,
        }
    }

    fn new_inclusive<B1, B2>(low: B1, high: B2) -> Self
    where
        B1: SampleBorrow<Self::X> + Sized,
        B2: SampleBorrow<Self::X> + Sized,
    {
        let low = low.borrow();
        let high = high.borrow();
        assert!(low <= high);

        UniformNodeId {
            low: *low,
            high: *high,
            inclusive: true,
        }
    }

    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> NodeId {
        let mut out = NodeId::new();
        let low_is_zero = self.low.is_zero();
        loop {
            rng.fill(&mut out[..]);
            if self.inclusive {
                if out <= self.high && (low_is_zero || out >= self.low) {
                    break out;
                }
            } else if out < self.high && (low_is_zero || out >= self.low) {
                break out;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_hex() {
        let h = b"3F3F3F3F3F3F3F3F3F3F3F3F3F3F3F3F3F3F3F3F";
        let n = NodeId::from_hex(h).unwrap();
        assert_eq!(NodeId::all(0x3F), n);
    }

    #[test]
    fn encode_hex() {
        let n = NodeId::all(0x3F);
        let mut buf = [0; 40];
        n.encode_hex(&mut buf).unwrap();
        assert_eq!(b"3F3F3F3F3F3F3F3F3F3F3F3F3F3F3F3F3F3F3F3F"[..], buf[..]);
    }

    #[test]
    fn sort_order() {
        let mut a = [NodeId::all(0), NodeId::all(3), NodeId::all(1)];
        a.sort();
        assert_eq!([NodeId::all(0), NodeId::all(1), NodeId::all(3)], a);
    }

    #[test]
    fn xor() {
        let a = NodeId::all(0b0000_0101);
        let b = NodeId::all(0b1100_0100);
        let c = a ^ b;
        assert_eq!(NodeId::all(0b1100_0001), c);
    }

    #[test]
    fn test_gen_lz() {
        let n = NodeId::gen_lz(5);
        assert!(n.lz() >= 5);
    }

    #[test]
    fn test_mask_lz() {
        let actual = NodeId::mask_lz(5);
        let mut expected = NodeId::max();
        expected[0] = 0b0000_0111;
        assert_eq!(5, actual.lz());
        assert_eq!(expected, actual);

        let actual = NodeId::mask_lz(8);
        let mut expected = NodeId::max();
        expected[0] = 0;
        assert_eq!(8, actual.lz());
        assert_eq!(expected, actual);

        let actual = NodeId::mask_lz(9);
        let mut expected = NodeId::max();
        expected[0] = 0;
        expected[1] = 0b0111_1111;
        assert_eq!(9, actual.lz());
        assert_eq!(expected, actual);
    }
}
