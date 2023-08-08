/*! Text&Metadata writer for a given language.

Holds writing and rotating on both text and metadata files for a given language.
Supports writing of numerous [MergedPiece], given that their identification are the same.
Identification is checked too, preventing the writing of differently identified [MergedPiece] into a given language writer.
!*/
use std::io::Write;
use std::path::Path;

use oxilangtag::LanguageTag;

use crate::v3::Document;

use crate::error;

use super::{
    new_writer::{Comp, NewWriter},
    MetaWriter, WriterTrait,
};

pub struct WriterDoc {
    handle: NewWriter,
}

impl WriterDoc {
    pub fn flush(&mut self) -> Result<(), std::io::Error> {
        self.handle.flush()
    }
}

impl WriterTrait for WriterDoc {
    type Item = Document;
    /// Create a new Writer for provided language.
    /// Files will be written at the root of the `dst` file, and shouldn't exceed `size_limit`.
    ///
    /// _See [TextWriter] to have an explanation about the *shouldn't*._
    fn new(
        dst: &Path,
        lang: LanguageTag<String>,
        _size_limit: Option<u64>,
        comp: Option<Comp>,
    ) -> Result<Self, error::Error> {
        Ok(Self {
            handle: NewWriter::new(
                dst,
                lang.to_string(),
                // Some(Comp::Zstd { level: 0 }),
                comp,
                _size_limit.map(|x| x as usize),
            )?,
        })
    }
    /// writes the provided [MergedPiece], checking language identification.
    fn write(&mut self, pieces: Vec<Document>) -> Result<(), error::Error> {
        let mut piece_str = String::new();
        for piece in pieces {
            piece_str += &serde_json::to_string(&piece)?;
            piece_str.push('\n');
        }
        self.handle.write_all(piece_str.as_bytes())?;

        Ok(())
    }

    fn write_single(&mut self, piece: &Document) -> Result<(), error::Error> {
        Ok(serde_json::to_writer(&mut self.handle, piece)?)
    }
    // Binds to [MetaWriter::close_file].
    // Closes current metadata file.
    // TODO: put this in impl Drop?
    fn close_meta(&mut self) -> Result<(), error::Error> {
        todo!()
    }
}
#[cfg(test)]
mod tests {

    use std::{collections::HashMap, fs::File, path::PathBuf};

    use oxilangtag::LanguageTag;
    use warc::WarcHeader;

    use crate::v3::{Document, Metadata};

    use super::*;
    use crate::common::Identification;

    type WarcHeaders = HashMap<WarcHeader, Vec<u8>>;

    #[test]
    fn test_init() {
        let dst = Path::new("dst_test_init_writer");
        std::fs::create_dir(dst).unwrap();
        let _ = WriterDoc::new(
            dst,
            LanguageTag::parse("en".to_string()).unwrap(),
            Some(1_000_000),
            None,
        );
        std::fs::remove_dir_all(dst).unwrap();
    }

    #[test]
    fn write() {
        let dst = tempfile::tempdir().unwrap();
        let mut wr = WriterDoc::new(
            dst.path(),
            LanguageTag::parse("fr".to_string()).unwrap(),
            Some(10),
            None,
        )
        .unwrap();

        let headers: WarcHeaders =
            vec![(WarcHeader::Filename, Vec::from("filenametest".as_bytes()))]
                .into_iter()
                .collect();

        let sentences = "Bonjour, c'est moi!
Comment allez-vous?
Bien, et vous?
Ecoutez ça va plutôt bien.";

        let id = Identification::new(LanguageTag::parse("en".to_string()).unwrap(), 1.0);
        let ids = vec![Some(id.clone()), Some(id.clone()), Some(id.clone())];
        let metadata = Metadata::new(&id, &ids);
        let doc = vec![Document::new(sentences.to_string(), headers, metadata)];

        wr.write(doc.clone()).unwrap();
        wr.handle.flush();

        // check if content is the same
        let _sentences = String::new();
        let pathd = PathBuf::from(dst.path()).join("fr.jsonl");
        dbg!(&pathd);
        // std::thread::sleep(std::time::Duration::from_secs(50));
        let f = File::open(&pathd).unwrap();

        dbg!(std::fs::read_to_string(&pathd));
        let document: Document = serde_json::from_reader(&f).unwrap();
        let sentences = document.content();
        //to account for \n\n
        let from_merged_pieces = doc[0].content().clone();

        assert_eq!(sentences, &from_merged_pieces);

        std::fs::remove_dir_all(dst).unwrap();
    }

    #[test]
    fn test_newline_bug() {
        // create a possibly faulty document
        let content = r#"hel\nlo\r\n"#.to_string();
        let headers = HashMap::new();
        let meta = Metadata::new(
            &Identification::new(LanguageTag::parse("en".to_string()).unwrap(), 1.0f32),
            &*vec![Some(Identification::new(
                LanguageTag::parse("en".to_string()).unwrap(),
                1.0f32,
            ))],
        );
        let doc = Document::new(content, headers, meta);

        // check that we have the correct number of ids
        assert_eq!(
            doc.content().lines().count(),
            doc.metadata().sentence_identifications().len()
        );

        let dst = tempfile::tempdir().unwrap();
        let mut wr = WriterDoc::new(
            dst.path(),
            LanguageTag::parse("fr".to_string()).unwrap(),
            Some(10),
            None,
        )
        .unwrap();

        wr.write(vec![doc.clone()]).unwrap();
        wr.flush().unwrap();
        let pathd = PathBuf::from(dst.path()).join("fr.jsonl");
        let f = File::open(pathd).unwrap();

        let doc_from_ser: Document = serde_json::from_reader(&f).unwrap();

        assert_eq!(doc, doc_from_ser);
    }
}
