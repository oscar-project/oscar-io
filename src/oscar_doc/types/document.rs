use serde::{Deserialize, Serialize};

use crate::common::Identification;

use super::{Metadata, WarcHeaders};

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
        self.metadata().identification()
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
    pub fn metadata_mut(&mut self) -> &mut Metadata {
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

#[cfg(test)]
mod tests {
    // TODO
}
