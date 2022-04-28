//! OSCAR Schema v2 (OSCAR 22.01) types, readers and writers.
//!
//! Each document is materialized by a [Document], holding [Metadata], [WarcHeaders] and `content` (that is a [String]).
mod reader;
mod types;
mod write;

#[cfg(feature = "avro")]
pub use reader::AvroDocReader as AvroReader;
pub use reader::DocReader as Reader;
pub use reader::SplitFileIter as SplitReader;
pub use reader::SplitFolderFileIter as SplitFolderReader;
pub use types::Document;
pub use types::Metadata;
pub use types::WarcHeaders;
pub use write::DocWriter as Writer;
