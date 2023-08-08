//! Writer for OSCAR Schema v2/3
//!
//! TODO: Refactor it a bit.
//!
//! The module is messy because OSCAR Schema v3 writer/reader is copied from metadata R/W from v1.1.
mod docwriter;
mod writer;
mod writertrait;

pub use docwriter::DocWriter as Writer;
pub use writer::Comp;
pub use writertrait::WriterTrait;
