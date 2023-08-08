//! Writer for OSCAR Schema v2/3
//!
//! TODO: Refactor it a bit.
//!
//! The module is messy because OSCAR Schema v3 writer/reader is copied from metadata R/W from v1.1.
mod metawriter;
mod new_writer;
mod writer;
mod writertrait;

use metawriter::MetaWriter;
pub use new_writer::Comp;
pub use writer::WriterDoc as Writer;
pub use writertrait::WriterTrait;
