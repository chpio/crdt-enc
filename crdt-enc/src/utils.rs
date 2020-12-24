mod version_bytes;

pub use version_bytes::*;

use crdts::{CmRDT, CvRDT};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EmptyCrdt;

impl CmRDT for EmptyCrdt {
    type Op = ();

    fn apply(&mut self, _op: Self::Op) {}
}

impl CvRDT for EmptyCrdt {
    fn merge(&mut self, _other: Self) {}
}
