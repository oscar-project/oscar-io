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
                required group annotations (LIST) {
                    repeated group list {
                        optional binary annotation (UTF8);
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
    #[derive(Debug)]
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

    pub fn write_docs(&mut self, docs: &[Document]) -> Result<(), Error> {
        let doc_grouped = DocGroup::new(docs);

        // iterate on each column and write
        todo!()
    }
}

#[derive(Debug)]
struct DocGroup<'a> {
    contents: Vec<&'a str>,
    warc_headers: Vec<&'a HashMap<String, String>>,
    annotations: Vec<&'a Option<Vec<String>>>,
    ids: Vec<&'a Identification>,
    line_ids: Vec<&'a [Option<Identification>]>,
    nb_col: usize,
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
            nb_col: 0,
        }
    }
}

// impl<'a> Iterator for DocGroup<'a> {
//     type Item = DocGroupPart<'a>;

//     fn next(&mut self) -> Option<Self::Item> {
//         match self.nb_col {
//             0 => Some(DocGroupPart::Contents(&self.contents)),
//             1 => Some(DocGroupPart::Warcs(&self.warc_headers)),
//             2 => Some(DocGroupPart::Annotations(&self.annotations)),
//             3 => Some(DocGroupPart::Id(&self.ids)),
//             4 => Some(DocGroupPart::LineIds(&self.line_ids)),
//             _ => None,
//         }
//     }
// }
struct DocumentFieldsIterator<'a> {
    inner: &'a Document,
    part_nb: usize,
}

#[derive(Debug, PartialEq)]
enum DocPart<'a> {
    Content(&'a str),
    Warc(&'a HashMap<String, String>),
    Annotation(&'a Option<Vec<String>>),
    Id(&'a Identification),
    LineIds(&'a [Option<Identification>]),
}

enum DocGroupPart<'a> {
    Contents(&'a Vec<&'a str>),
    Warcs(&'a Vec<&'a HashMap<String, String>>),
    Annotations(&'a Vec<&'a Option<Vec<String>>>),
    Id(&'a Vec<&'a Identification>),
    LineIds(&'a Vec<&'a [Option<Identification>]>),
}
impl<'a> Iterator for DocumentFieldsIterator<'a> {
    type Item = DocPart<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let ret = match self.part_nb {
            0 => Some(DocPart::Content(self.inner.content())),
            1 => Some(DocPart::Warc(self.inner.warc_headers())),
            2 => Some(DocPart::Annotation(self.inner.metadata().annotation())),
            3 => Some(DocPart::Id(self.inner.identification())),
            4 => Some(DocPart::LineIds(
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
mod test_writer {
    use std::{fs::File, sync::Arc};

    use parquet::{
        file::{
            properties::WriterProperties,
            writer::{FileWriter, InMemoryWriteableCursor, SerializedFileWriter},
        },
        schema::{parser::parse_message_type, types::Type},
    };

    use crate::oscar_doc::write::writer_parquet::SCHEMA;

    use super::ParquetWriter;

    #[test]
    fn test_tiny_nested() {
        let schema = r#"
            message metadata {
                repeated group sentence_identifications {
                        required binary lang (UTF8);
                        required float prob;
                }
            }
        "#;

        // lang
        //   DEF: 1 (required)
        //   REP: 1 (sentence_ids is repeated)
        // prob
        //   DEF: 1 (required)
        //   REP: 1 (sentence_ids is repeated)
        let schema = parse_message_type(schema).unwrap();
        print_arbo(&schema, 2);
        let buf = InMemoryWriteableCursor::default();
        let mut buf = File::create("./test.parquet").unwrap();
        let props = WriterProperties::builder().build();
        let mut w =
            SerializedFileWriter::new(buf, Arc::new(schema.clone()), Arc::new(props)).unwrap();

        let mut rg = w.next_row_group().unwrap();
        while let Some(mut col_writer) = rg.next_column().unwrap() {
            match col_writer {
                parquet::column::writer::ColumnWriter::BoolColumnWriter(ref mut a) => {
                    println!("bool")
                }
                parquet::column::writer::ColumnWriter::Int32ColumnWriter(ref mut a) => {
                    println!("int32")
                }
                parquet::column::writer::ColumnWriter::Int64ColumnWriter(_) => println!(),
                parquet::column::writer::ColumnWriter::Int96ColumnWriter(_) => println!(),
                parquet::column::writer::ColumnWriter::FloatColumnWriter(ref mut a) => {
                    // prob
                    println!("float");
                    let values: Vec<_> = (0..100i32).map(|x| x as f32).collect();
                    a.write_batch(&values, Some(&[1; 100]), Some(&[1; 100]))
                        .unwrap();
                }
                parquet::column::writer::ColumnWriter::DoubleColumnWriter(_) => println!("double"),
                parquet::column::writer::ColumnWriter::ByteArrayColumnWriter(ref mut a) => {
                    println!("bytearray");

                    // build strings, get view as str and into bytearray
                    let strs: Vec<_> = (0..100i32)
                        .map(|i| format!("lang_{i}").as_str().into())
                        .collect();
                    // let strs_borrow = strs.iter().collect();
                    let mut def_levels = vec![0; 1];
                    def_levels.append(&mut vec![1; 99]);
                    a.write_batch(&strs, Some(&def_levels), Some(&[1; 100]))
                        .unwrap();
                }
                parquet::column::writer::ColumnWriter::FixedLenByteArrayColumnWriter(_) => {
                    println!("fixedlenbytearray")
                }
            }
            rg.close_column(col_writer).unwrap();
        }
        w.close_row_group(rg).unwrap();
        w.close().unwrap();
    }
    #[test]
    fn test_simple_write() {
        let buf = InMemoryWriteableCursor::default();
        let w = ParquetWriter::new(buf, WriterProperties::builder().build()).unwrap();

        print_arbo(&*SCHEMA, 0);
    }

    fn print_arbo(node: &Type, indent: usize) {
        println!(
            "{}{} (cvt_type: {:?}, logic_type: {:?})",
            vec![" "; indent].join(""),
            node.name(),
            node.get_basic_info().converted_type(),
            node.get_basic_info().logical_type()
        );
        if let Type::GroupType {
            basic_info: _,
            fields: fields,
        } = node
        {
            for sub_node in fields {
                print_arbo(sub_node, indent + 4);
            }
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
        println!("{docgroup:#?}");
    }
}
#[cfg(test)]
mod test_doc_iter {
    use std::collections::HashMap;

    use crate::{
        common::Identification,
        lang::Lang,
        oscar_doc::{write::writer_parquet::DocPart, Document, Metadata},
    };

    #[test]
    fn foo() {
        let d = Document::new("hello!".to_string(), HashMap::new(), Metadata::default());
        let mut d_iter = d.iter_parquet();
        assert_eq!(d_iter.next(), Some(DocPart::Content(&"hello!".to_string())));
        assert_eq!(d_iter.next(), Some(DocPart::Warc(&HashMap::new())));
        assert_eq!(d_iter.next(), Some(DocPart::Annotation(&None)));
        assert_eq!(
            d_iter.next(),
            Some(DocPart::Id(&Identification::new(Lang::En, 1.0)))
        );
        assert_eq!(
            d_iter.next(),
            Some(DocPart::LineIds(&vec![Some(Identification::new(
                Lang::En,
                1.0
            ))]))
        );
    }
}
