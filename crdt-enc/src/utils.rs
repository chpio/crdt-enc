mod version_bytes;

pub use version_bytes::*;

use crdts::{CmRDT, CvRDT};
use serde::{Deserialize, Serialize};
use std::convert::Infallible;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EmptyCrdt;

impl CmRDT for EmptyCrdt {
    type Op = ();

    type Validation = Infallible;

    fn validate_op(&self, _op: &Self::Op) -> Result<(), Infallible> {
        Ok(())
    }

    fn apply(&mut self, _op: Self::Op) {}
}

impl CvRDT for EmptyCrdt {
    type Validation = Infallible;

    fn validate_merge(&self, _other: &Self) -> Result<(), Infallible> {
        Ok(())
    }

    fn merge(&mut self, _other: Self) {}
}
