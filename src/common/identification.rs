/*! Identifier trait

All identifiers should implement [Identifier] to be useable in processing and pipelines.
!*/
use std::ops::Deref;

use crate::error::Error;

use oxilangtag::{LanguageTag, LanguageTagParseError};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Identification<T: Deref<Target = str> + Clone> {
    label: LanguageTag<T>,
    prob: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IdentificationSer {
    label: String,
    prob: f32,
}

impl<T> From<Identification<T>> for IdentificationSer
where
    T: Deref<Target = str> + Clone,
{
    fn from(i: Identification<T>) -> Self {
        Self {
            label: i.label.to_string(),
            prob: i.prob,
        }
    }
}
impl TryFrom<IdentificationSer> for Identification<String> {
    type Error = LanguageTagParseError;
    fn try_from(i: IdentificationSer) -> Result<Self, Self::Error> {
        Ok(Self {
            label: LanguageTag::parse(i.label)?,
            prob: i.prob,
        })
    }
}

impl<T: Deref<Target = str> + Clone> Identification<T> {
    pub fn new(label: LanguageTag<T>, prob: f32) -> Self {
        Self { label, prob }
    }
    /// Get a reference to the identification's label.
    pub fn label(&self) -> &LanguageTag<T> {
        &self.label
    }

    /// Get a reference to the identification's prob.
    pub fn prob(&self) -> &f32 {
        &self.prob
    }
}

pub trait Identifier<T: Deref<Target = str> + Clone> {
    /// returns a language identification token (from [crate::lang::LANG]).
    fn identify(&self, sentence: T) -> Result<Option<Identification<T>>, Error>;
}

#[cfg(test)]
mod tests {
    // TODO tests
}
