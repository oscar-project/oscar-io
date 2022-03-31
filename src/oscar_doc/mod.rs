//! OSCAR Schema v2 (OSCAR 22.01) types, readers and writers.
//!
//! Each document is materialized by a [Document], holding [Metadata], [WarcHeaders] and `content` (that is a [String]).
mod reader;
mod types;
mod writer;

pub use reader::DocReader as Reader;
pub use types::Document;
pub use types::Metadata;
pub use types::WarcHeaders;
