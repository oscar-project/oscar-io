//! Parquet corpus writer
//!
//! The parquet files might not follow the hierarchy of usual jsonl files.
//! See [here](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md) for parquet logicaltypes
//! record_id ->

use std::{
    collections::HashMap,
    io::{Seek, Write},
    sync::Arc,
};

use crate::oscar_doc::types::Document;
use crate::{common::Identification, error::Error};
use lazy_static::lazy_static;
use parquet::{
    data_type::ByteArray,
    file::{properties::WriterProperties, writer::SerializedFileWriter},
    schema::{parser::parse_message_type, types::Type},
};

const DOCUMENT_SCHEMA: &'static str = r#"
message document {
    required group lines (LIST) {
        repeated binary line (UTF8);
    }
    required binary label (UTF8);
    required float prob;
    }
}"#;

/// Parquet Row.
#[derive(Debug)]
struct Row<T>(Vec<T>, Vec<i16>, Vec<i16>);

/// Parquet-compatible document
#[derive(Debug)]
struct SimpleDoc {
    lines: Vec<ByteArray>,
    label: ByteArray,
    prob: f32,
}

impl SimpleDoc {
    fn into_raw_parts(self) -> (Vec<ByteArray>, ByteArray, f32) {
        (self.lines, self.label, self.prob)
    }
}
impl From<Document> for SimpleDoc {
    fn from(d: Document) -> Self {
        let lines = d.content().lines().map(|x| x.into()).collect();
        let label = d.identification().label().to_static().into();
        let prob = *d.identification().prob();
        Self { lines, label, prob }
    }
}

/// Row-major "view" of a set of docs.
/// Not really a view per se, since docs are moved and some processing is done.
#[derive(Debug)]
struct SimpleDocsRow {
    line_sets: Vec<Vec<ByteArray>>,
    labels: Vec<ByteArray>,
    probs: Vec<f32>,
}

impl From<Vec<SimpleDoc>> for SimpleDocsRow {
    fn from(docs: Vec<SimpleDoc>) -> Self {
        let cap = docs.len();
        let raw_data = docs.into_iter().map(SimpleDoc::into_raw_parts);
        let mut line_sets = Vec::with_capacity(cap);
        let mut labels = Vec::with_capacity(cap);
        let mut probs = Vec::with_capacity(cap);
        for (lines, label, prob) in raw_data {
            line_sets.push(lines);
            labels.push(label);
            probs.push(prob);
        }

        Self {
            line_sets,
            labels,
            probs,
        }
    }
}

impl From<Vec<Document>> for SimpleDocsRow {
    fn from(docs: Vec<Document>) -> Self {
        Self::from(
            docs.into_iter()
                .map(|x| x.into())
                .collect::<Vec<SimpleDoc>>(),
        )
    }
}

impl SimpleDocsRow {
    pub fn lines(&self) -> Row<ByteArray> {
        let cap = self.line_sets.iter().fold(0, |acc, x| acc + x.len());
        let mut vals = Vec::with_capacity(cap);
        let mut rep = Vec::with_capacity(cap);

        // always defined.
        let def = vec![1; cap];

        for line_set in &self.line_sets {
            rep.push(0);
            rep.extend(vec![1; line_set.len() - 1]);

            vals.extend(line_set.iter().cloned());
        }

        Row(vals, def, rep)
    }

    pub fn labels(&self) -> Row<ByteArray> {
        let cap = self.labels.len();
        let mut rep = Vec::with_capacity(cap);

        let vals = self.labels.clone();
        let def = vec![0; cap];
        rep.push(0);
        rep.extend(vec![0; cap - 1]);

        Row(vals, def, rep)
    }

    pub fn probs(&self) -> Row<f32> {
        let cap = self.probs.len();
        let mut vals = Vec::with_capacity(cap);
        let mut def = Vec::with_capacity(cap);
        let mut rep = Vec::with_capacity(cap);

        vals = self.probs.clone();
        def = vec![0; cap];
        rep.push(0);
        rep.extend(vec![0; cap - 1]);

        Row(vals, def, rep)
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, str::FromStr, sync::Arc};

    use parquet::{
        data_type::{ByteArrayType, FloatType},
        file::{properties::WriterProperties, writer::SerializedFileWriter},
        schema::parser::parse_message_type,
    };

    use crate::{
        lang::Lang,
        oscar_doc::{Document, Metadata},
    };

    use super::{Row, SimpleDoc, SimpleDocsRow, DOCUMENT_SCHEMA};

    fn sample_docs() -> SimpleDocsRow {
        let docs = vec![
            Document::new(
                "0foo\nbar\nbaz".to_string(),
                HashMap::default(),
                Metadata::default(),
            ),
            Document::new(
                "1foo\nbar\nbaz".to_string(),
                HashMap::default(),
                Metadata::default(),
            ),
            Document::new(
                "2foo\nbar\nbaz".to_string(),
                HashMap::default(),
                Metadata::default(),
            ),
            Document::new(
                "3foo\nbar\nbaz".to_string(),
                HashMap::default(),
                Metadata::default(),
            ),
            Document::new(
                "4foo\nbar\nbaz".to_string(),
                HashMap::default(),
                Metadata::default(),
            ),
        ];

        SimpleDocsRow::from(docs)
    }
    // check conv from/to simpledoc
    #[test]
    fn test_simpledoc() {
        let d = Document::new(
            "foo\nbar\nbaz".to_string(),
            HashMap::default(),
            Metadata::default(),
        );

        let sd: SimpleDoc = d.clone().into();

        // convert back to native types for assertion
        let content = sd
            .lines
            .into_iter()
            .map(|line| line.as_utf8().unwrap().to_string())
            .collect::<Vec<String>>()
            .join("\n");
        let label = Lang::from_str(sd.label.as_utf8().unwrap()).unwrap();
        let prob = sd.prob;

        assert_eq!(&content, d.content());
        assert_eq!(&label, d.identification().label());
        assert_eq!(&prob, d.identification().prob());
    }

    #[test]
    fn test_into_rows() {
        let docs = sample_docs();
        println!("{docs:#?}");
        println!("{:#?}", docs.lines());

        assert!(false); //TODO
    }

    #[test]
    #[ignore]
    fn test_write_parquet() {
        let docs = sample_docs();
        println!("{:#?}", docs.lines());
        println!("{:#?}", docs.probs());
        println!("{:#?}", docs.labels());
        let schema = parse_message_type(DOCUMENT_SCHEMA).unwrap();
        let file = std::fs::File::create("/Users/jabadji/test.parquet").unwrap();
        let mut writer = SerializedFileWriter::new(
            file,
            Arc::new(schema),
            Arc::new(WriterProperties::builder().build()),
        )
        .unwrap();

        let mut rg = writer.next_row_group().unwrap();
        let mut nb_col = 0;

        while let Some(mut col_writer) = rg.next_column().unwrap() {
            match nb_col {
                0 => {
                    let cw = col_writer.typed::<ByteArrayType>();
                    let Row(values, def_levels, rep_levels) = docs.lines();
                    cw.write_batch(&values, Some(&def_levels), Some(&rep_levels))
                        .unwrap();
                }
                1 => {
                    let cw = col_writer.typed::<ByteArrayType>();
                    let Row(values, def_levels, rep_levels) = docs.labels();
                    cw.write_batch(&values, Some(&def_levels), Some(&rep_levels))
                        .unwrap();
                }
                2 => {
                    let cw = col_writer.typed::<FloatType>();
                    let Row(values, def_levels, rep_levels) = docs.probs();
                    cw.write_batch(&values, Some(&def_levels), Some(&rep_levels))
                        .unwrap();
                }
                _ => panic!("no rows to write!"),
            }
            col_writer.close().unwrap();
            nb_col += 1;
        }
        rg.close().unwrap();
        writer.close().unwrap();
    }
}
