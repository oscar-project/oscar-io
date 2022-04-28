//! Parquet corpus writer
//!

use std::{
    collections::HashMap,
    io::{Seek, Write},
    sync::Arc,
};

use crate::oscar_doc::types::Document;
use crate::{common::Identification, error::Error};
use lazy_static::lazy_static;
use parquet::{
    file::{
        properties::WriterProperties,
        writer::{SerializedFileWriter, TryClone},
    },
    schema::{parser::parse_message_type, types::Type},
};
const DOCUMENT_SCHEMA: &'static str = "
        message document {
            REQUIRED BYTE_ARRAY content (UTF8);
            REQUIRED group warc_headers (MAP) {
                required binary header (UTF8);
                required binary value (UTF8);
            }
            required group metadata {
                required group identification {
                    required binary lang (UTF8);
                    required float id;
                }
                required group annotation (LIST) {
                    repeated group list {
                        optional binary element (UTF8);
                    }
                }
                required group sentence_identifications (LIST) {
                    repeated group list {
                        required binary lang (UTF8);
                        required float id;
                    }
                }
            }
        }
        ";
lazy_static! {
    pub static ref SCHEMA: Type = parse_message_type(DOCUMENT_SCHEMA).expect("invalid schema");
}

pub struct ParquetWriter<W: Write + Seek + TryClone> {
    writer: SerializedFileWriter<W>,
}

impl<W: Write + Seek + TryClone> ParquetWriter<W> {
    pub fn new(writer: W, props: WriterProperties) -> Result<Self, parquet::errors::ParquetError> {
        Ok(Self {
            writer: SerializedFileWriter::new(writer, Arc::new(SCHEMA.clone()), Arc::new(props))?,
        })
    }

    pub fn write_docs(docs: &[Document]) -> Result<(), Error> {
        // docs.into_iter().map(|doc| doc.iter_parquet()).
        todo!()
    }
}

#[derive(Debug)]
struct DocGroup<'a> {
    contents: Vec<&'a str>,
    warc_headers: Vec<&'a HashMap<String, String>>,
    //FIXME: use [String]
    annotations: Vec<&'a Option<Vec<String>>>,
    ids: Vec<&'a Identification>,
    line_ids: Vec<&'a [Option<Identification>]>,
}

impl<'a> DocGroup<'a> {
    pub fn new(docs: &'a [Document]) -> Self {
        let mut contents = Vec::new();
        let mut warc_headers = Vec::new();
        let mut annotations = Vec::new();
        let mut ids = Vec::new();
        let mut line_ids = Vec::new();
        for d in docs {
            contents.push(d.content().as_str());
            warc_headers.push(d.warc_headers());
            annotations.push(d.metadata().annotation());
            ids.push(d.metadata().identification());
            line_ids.push(d.metadata().sentence_identifications());
        }

        Self {
            contents,
            warc_headers,
            annotations,
            ids,
            line_ids,
        }
    }
}

struct DocumentFieldsIterator<'a> {
    inner: &'a Document,
    part_nb: usize,
}

#[derive(Debug, PartialEq)]
enum DocumentPart<'a> {
    Content(&'a String),
    Warc(&'a HashMap<String, String>),
    Annotation(&'a Option<Vec<String>>),
    Id(&'a Identification),
    LineIds(&'a [Option<Identification>]),
}
impl<'a> Iterator for DocumentFieldsIterator<'a> {
    type Item = DocumentPart<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let ret = match self.part_nb {
            0 => Some(DocumentPart::Content(self.inner.content())),
            1 => Some(DocumentPart::Warc(self.inner.warc_headers())),
            2 => Some(DocumentPart::Annotation(self.inner.metadata().annotation())),
            3 => Some(DocumentPart::Id(self.inner.identification())),
            4 => Some(DocumentPart::LineIds(
                self.inner.metadata().sentence_identifications(),
            )),
            _ => None,
        };
        self.part_nb += 1;
        ret
    }
}
impl Document {
    fn iter_parquet(&self) -> DocumentFieldsIterator {
        DocumentFieldsIterator {
            inner: &self,
            part_nb: 0,
        }
    }
}

#[cfg(test)]
mod test_doc_group {
    use std::collections::HashMap;

    use crate::oscar_doc::{Document, Metadata};

    use super::DocGroup;

    #[test]
    fn from_vec() {
        let docs: Vec<Document> = ["hello", "second document", "third document\n :)"]
            .into_iter()
            .map(|content| Document::new(content.to_string(), HashMap::new(), Metadata::default()))
            .collect();

        let docgroup = DocGroup::new(&docs);
        println!("{docgroup:?}");
    }
}
#[cfg(test)]
mod test_doc_iter {
    use std::collections::HashMap;

    use crate::{
        common::Identification,
        lang::Lang,
        oscar_doc::{write::writer_parquet::DocumentPart, Document, Metadata},
    };

    #[test]
    fn foo() {
        let d = Document::new("hello!".to_string(), HashMap::new(), Metadata::default());
        let mut d_iter = d.iter_parquet();
        assert_eq!(
            d_iter.next(),
            Some(DocumentPart::Content(&"hello!".to_string()))
        );
        assert_eq!(d_iter.next(), Some(DocumentPart::Warc(&HashMap::new())));
        assert_eq!(d_iter.next(), Some(DocumentPart::Annotation(&None)));
        assert_eq!(
            d_iter.next(),
            Some(DocumentPart::Id(&Identification::new(Lang::En, 1.0)))
        );
        assert_eq!(
            d_iter.next(),
            Some(DocumentPart::LineIds(&vec![Some(Identification::new(
                Lang::En,
                1.0
            ))]))
        );
    }
}
