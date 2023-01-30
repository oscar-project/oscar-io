use oxilangtag::LanguageTag;
use serde::{Deserialize, Serialize};

use crate::common::Identification;

/// OSCAR Metadata.
/// Contains document identification, annotations and sentence-level identifications.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Metadata {
    identification: Identification<String>,
    annotation: Option<Vec<String>>,
    sentence_identifications: Vec<Option<Identification<String>>>,
}

impl Metadata {
    /// Create a new [Metadata].
    /// Internally clones provided parameters
    pub fn new(
        identification: &Identification<String>,
        annotation: &Option<Vec<String>>,
        sentence_identifications: &[Option<Identification<String>>],
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
    pub fn identification(&self) -> &Identification<String> {
        &self.identification
    }

    /// Get a reference to the metadata's annotation.
    pub fn annotation(&self) -> Option<&Vec<String>> {
        self.annotation.as_ref()
    }
}

impl Default for Metadata {
    /// default Metadata is English with 1.0 prob,
    /// no annotation and a single english sentence with 1.0 prob.
    fn default() -> Self {
        let default_tag = LanguageTag::parse("en".to_string()).unwrap();
        Self {
            identification: Identification::new(default_tag.clone(), 1.0),
            annotation: None,
            sentence_identifications: vec![Some(Identification::new(default_tag, 1.0))],
        }
    }
}
