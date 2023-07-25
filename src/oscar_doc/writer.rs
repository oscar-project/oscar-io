//! Document writer. Does only implement simple write for now.
use std::io::Write;

use crate::error::Error;

use crate::oscar_doc::Document;

pub struct DocWriter<W: Write> {
    w: W,
}

//TODO: DocWriter on GzEncoder

impl<W: Write> DocWriter<W> {
    pub fn new(writer: W) -> Self {
        Self { w: writer }
    }

    /// Serializes the document as a [String], adds a newline and calls [std::io::Write] in the inner writer.
    ///
    /// Does not call [Self::flush], so be careful of calling it after writing
    pub fn write(&mut self, doc: &Document) -> Result<(), Error> {
        let write_bytes = serde_json::to_string(doc)? + "\n";
        self.w.write_all(write_bytes.as_bytes())?;

        Ok(())
    }

    /// calls [Self::write] for each document, returning an error if there's any failure, then calls [Self::flush].
    pub fn write_multiple(&mut self, docs: &[Document]) -> Result<(), Error> {
        for doc in docs {
            self.write(doc)?;
        }
        self.flush()?;
        Ok(())
    }

    /// Maps to [std::io::Write::flush] method on the inner writer.
    pub fn flush(&mut self) -> Result<(), Error> {
        Ok(self.w.flush()?)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs::File,
        io::{BufReader, Cursor},
    };

    use crate::oscar_doc::Document;
    use crate::oscar_doc::Reader;

    use super::DocWriter;

    fn get_docs() -> Vec<Document> {
        let f = File::open("tests/res/data.jsonl").unwrap();
        let r = BufReader::new(f);
        let cr = Reader::new(r);
        cr.map(|x| x.unwrap()).collect()
    }

    #[test]
    fn test_write_simple() {
        let mut writer = vec![];
        let mut dw = DocWriter::new(&mut writer);

        let docs = get_docs();

        // write docs
        for doc in &docs {
            dw.write(doc).unwrap();
        }

        dw.flush().unwrap();

        // read from buffer
        let c = Cursor::new(&mut writer);
        let mut br = BufReader::new(c);
        let reader = Reader::new(&mut br);

        // map results to Ok, crashing if Error
        let docs_from_reader: Vec<Document> = reader.map(|x| x.unwrap()).collect();

        assert!(docs.len() != 0);
        assert!(docs_from_reader.len() != 0);
        assert_eq!(docs, docs_from_reader);
    }

    #[test]
    fn test_write_multiple() {
        let mut writer = vec![];
        let mut dw = DocWriter::new(&mut writer);

        let docs = get_docs();

        // write docs
        dw.write_multiple(&docs).unwrap();

        // read from buffer
        let c = Cursor::new(&mut writer);
        let mut br = BufReader::new(c);
        let reader = Reader::new(&mut br);

        // map results to Ok, crashing if Error
        let docs_from_reader: Vec<Document> = reader.map(|x| x.unwrap()).collect();

        assert!(docs.len() != 0);
        assert!(docs_from_reader.len() != 0);
        assert_eq!(docs, docs_from_reader);
    }
}
