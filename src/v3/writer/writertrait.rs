use std::path::Path;

use oxilangtag::LanguageTag;

use crate::error::Error;

use super::writer::Comp;

pub trait WriterTrait {
    type Item;

    fn new(
        dst: &Path,
        lang: LanguageTag<String>,
        max_file_size: Option<u64>,
        comp: Option<Comp>,
    ) -> Result<Self, Error>
    where
        Self: Sized;
    fn write(&mut self, vals: Vec<Self::Item>) -> Result<(), Error>;
    fn write_single(&mut self, val: &Self::Item) -> Result<(), Error>;
}
