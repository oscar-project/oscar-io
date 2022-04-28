use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{common::Identification, lang::Lang};

/// OSCAR Metadata.
/// Contains document identification, annotations and sentence-level identifications.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, JsonSchema)]
pub struct Metadata {
    identification: Identification,
    annotation: Option<Vec<String>>,
    sentence_identifications: Vec<Option<Identification>>,
}

impl Metadata {
    /// Create a new [Metadata].
    /// Internally clones provided parameters
    pub fn new(
        identification: &Identification,
        annotation: &Option<Vec<String>>,
        sentence_identifications: &[Option<Identification>],
    ) -> Self {
        Metadata {
            identification: identification.clone(),
            annotation: annotation.to_owned(),
            sentence_identifications: sentence_identifications.to_owned(),
        }
    }

    /// Adds an annotation.
    pub fn add_annotation(&mut self, annotation: String) {
        match &mut self.annotation {
            Some(anno) => anno.push(annotation),
            None => self.annotation = Some(vec![annotation]),
        }
    }

    /// Get a reference to the identification
    pub fn identification(&self) -> &Identification {
        &self.identification
    }

    /// Get a reference to the metadata's annotation.
    // pub fn annotation(&self) -> Option<&Vec<String>> {
    pub fn annotation(&self) -> &Option<Vec<String>> {
        &self.annotation
    }

    /// Get a reference to the metadata's sentence identifications.
    #[must_use]
    pub fn sentence_identifications(&self) -> &[Option<Identification>] {
        self.sentence_identifications.as_ref()
    }
}

impl Default for Metadata {
    /// default Metadata is English with 1.0 prob,
    /// no annotation and a single english sentence with 1.0 prob.
    fn default() -> Self {
        Self {
            identification: Identification::new(Lang::En, 1.0),
            annotation: None,
            sentence_identifications: vec![Some(Identification::new(Lang::En, 1.0))],
        }
    }
}
