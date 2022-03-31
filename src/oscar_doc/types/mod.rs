//! OSCAR Doc types (Document, Metadata...)
mod document;
mod metadata;

/// Simple alias to a HashMap<String, String> for simplifying the sourcecode.
///
/// WarcHeaders are usually HashMap<String, Vec<u8>> in the original implementation.
pub type WarcHeaders = HashMap<String, String>;
use std::collections::HashMap;

pub use document::Document;
pub use metadata::Metadata;
