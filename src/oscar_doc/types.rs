use std::collections::HashMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::common::Identification;
use crate::lang::Lang;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, JsonSchema)]
pub struct Metadata {
    identification: Identification,
    annotation: Option<Vec<String>>,
    sentence_identifications: Vec<Option<Identification>>,
}

impl Metadata {
    pub fn new(
        identification: &Identification,
        sentence_identifications: &[Option<Identification>],
    ) -> Self {
        Metadata {
            identification: identification.clone(),
            annotation: None,
            sentence_identifications: sentence_identifications.to_owned(),
        }
    }

    /// Set the metadata's annotation.
    pub fn set_annotation(&mut self, annotation: String) {
        match &mut self.annotation {
            Some(anno) => anno.push(annotation),
            None => self.annotation = Some(vec![annotation]),
        }
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
        Self {
            identification: Identification::new(Lang::En, 1.0),
            annotation: None,
            sentence_identifications: vec![Some(Identification::new(Lang::En, 1.0))],
        }
    }
}
pub type WarcHeaders = HashMap<String, String>;

/// A Document is a structure holding content, WARC headers and OSCAR-specific metadata.
/// - TODO: Change warc_headers from [RawRecordHeader] to [warc::Record] with [warc::EmptyBody]?
/// This way we shouldn't have to parse strings or use unwrap on [RawRecordHeader].
#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub struct Document {
    content: String,
    warc_headers: WarcHeaders,
    metadata: Metadata,
}

impl Document {
    pub fn new(content: String, warc_headers: WarcHeaders, metadata: Metadata) -> Self {
        Self {
            content,
            warc_headers,
            metadata,
        }
    }

    /// Instantiate a Document from a record and a related metadata.

    /// Get a reference to the Document's identification
    pub fn identification(&self) -> &Identification {
        &self.metadata.identification
    }

    /// Get a reference to the content
    pub fn content(&self) -> &String {
        &self.content
    }

    /// Get a reference to the document's warc headers.
    pub fn warc_headers(&self) -> &WarcHeaders {
        &self.warc_headers
    }

    /// Get a mutable reference to the document's metadata.
    pub(crate) fn metadata_mut(&mut self) -> &mut Metadata {
        &mut self.metadata
    }

    /// Get a reference to the document's metadata.
    pub(crate) fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    /// Set the document's content.
    pub fn set_content(&mut self, content: String) {
        self.content = content;
    }
}

/// custom debug implementation that converts:
/// - `headers` from [Vec<u8>] to [String] for easier readablility
/// - `content` from [String] to [Vec<String>] to better diagnose identification
// impl std::fmt::Debug for Document {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         let headers_pp: HashMap<String, String> = self
//             .warc_headers
//             .iter()
//             .map(|(k, v)| (k.clone(), v))
//             .collect();

//         let lines = &self.content.lines().collect::<Vec<&str>>();
//         f.debug_struct("Document")
//             .field("content", &lines)
//             .field("warc_headers", &headers_pp)
//             .field("metadata", &self.metadata)
//             .finish()
//     }
// }

#[cfg(test)]
mod tests {
    use warc::{Record, WarcHeader};

    use super::{Document, Metadata};

    #[test]
    fn test_serialize() {
        let m = Metadata::default();

        let serialized = serde_json::to_string_pretty(&m).unwrap();

        println!("{}", serialized);

        let m2: Metadata = serde_json::from_str(&serialized).unwrap();

        println!("{:?}", m2);
    }
}
