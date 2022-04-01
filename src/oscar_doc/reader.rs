/*! OSCAR Schema v2 (22.01) Reader.

   Provides a way to read [Document]s from a [BufRead].

   TODO: Find a way to provide some reading of splitted corpora.
* !*/
use std::{
    fs::File,
    io::{BufRead, BufReader},
    path::PathBuf,
};

use flate2::bufread::MultiGzDecoder;

use crate::error::Error;

use super::types::Document;

/// Document reader.
/// The inner type has to implement [BufRead].
pub struct DocReader<R: BufRead> {
    r: R,
}

impl<R: BufRead> DocReader<R> {
    /// Create a new [DocReader].
    pub fn new(r: R) -> Self {
        DocReader { r }
    }
}

impl<R: BufRead> DocReader<BufReader<MultiGzDecoder<R>>> {
    pub fn from_gzip(r: R) -> Self {
        let dec = MultiGzDecoder::new(r);
        let br = BufReader::new(dec);
        DocReader::new(br)
    }
}

impl<R: BufRead> Iterator for DocReader<R> {
    type Item = Result<Document, Error>;

    /// Yields [Result]<[Document], [Error]>.
    /// Errors can be either [serde_json::Error] if the format is invalid, or [std::io::Error] if there has been some IO Error.
    fn next(&mut self) -> Option<Self::Item> {
        let mut s = String::new();
        match self.r.read_line(&mut s) {
            // stop if nothing is read
            Ok(0) => None,
            Ok(_) => {
                // Attempt to deserialize, map error to custom error enum if it fails
                let result: Result<Document, Error> =
                    serde_json::from_str(&s).map_err(|x| x.into());
                Some(result)
            }
            Err(e) => Some(Err(e.into())),
        }
    }
}

/// In the case where we have multiple splits for a given subcorpus.
struct SplitFileIter {
    //path to the directory
    //file names
    //counter
    base_path: PathBuf,
    file_name_start: String,
    file_name_end: String,
    file_name_extension: String,
    counter: usize,
}
impl SplitFileIter {
    fn new(
        base_path: PathBuf,
        file_name_start: String,
        file_name_end: String,
        file_name_extension: String,
        counter_start: usize,
    ) -> SplitFileIter {
        SplitFileIter {
            base_path,
            file_name_start,
            file_name_end,
            file_name_extension,
            counter: counter_start,
        }
    }
}

// TODO: Check with gzipped
impl Iterator for SplitFileIter {
    type Item = Result<BufReader<File>, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let filename = self.file_name_start.to_owned()
            + &self.counter.to_string()
            + &self.file_name_end
            + &self.file_name_extension;

        let mut full_path = self.base_path.clone();
        full_path.push(filename);

        match File::open(full_path) {
            // everything is ok, we return a bufreader
            Ok(f) => {
                let br = BufReader::new(f);
                self.counter += 1;
                Some(Ok(br))
            }

            // if the error is a NotFound, then we just arrived at the end
            // if not, there has been a problem.
            Err(e) => match e.kind() {
                std::io::ErrorKind::NotFound => None,
                _ => Some(Err(e.into())),
            },
        }
    }
}

#[cfg(test)]
mod tests {

    use std::io::{BufReader, Cursor, Write};

    use super::DocReader;
    use crate::{error::Error, oscar_doc::Document};
    use flate2::{bufread::MultiGzDecoder, write::GzEncoder, Compression};
    use std::io::BufRead;

    fn get_samples() -> &'static str {
        r#"{"content":"this is the main content","warc_headers":{"warc-type":"conversion","warc-date":"2021-09-16T11:37:01Z","warc-refers-to":"<urn:uuid:3cc5dbf1-6932-44e3-a5f9-87bddb242ed1>","warc-block-digest":"sha1:AAN5C7C7I2JOXM5ZYB5YNFPRC5N6GJES","content-type":"text/plain","warc-target-uri":"http://accueil-enfants-d-un-meme-pere.be/","content-length":"5095","warc-identified-content-language":"fra,eng","warc-record-id":"<urn:uuid:7c1c010a-61ca-4383-92ba-008390a56fc9>"},"metadata":{"identification":{"label":"fr","prob":0.9586384},"annotation":["short_sentences","header","footer"],"sentence_identifications":[{"label": "fr", "prob": 0.9}]}}
{"content":"this is the main content","warc_headers":{"warc-type":"conversion","warc-date":"2021-09-16T11:37:01Z","warc-refers-to":"<urn:uuid:3cc5dbf1-6932-44e3-a5f9-87bddb242ed1>","warc-block-digest":"sha1:AAN5C7C7I2JOXM5ZYB5YNFPRC5N6GJES","content-type":"text/plain","warc-target-uri":"http://accueil-enfants-d-un-meme-pere.be/","content-length":"5095","warc-identified-content-language":"fra,eng","warc-record-id":"<urn:uuid:7c1c010a-61ca-4383-92ba-008390a56fc9>"},"metadata":{"identification":{"label":"fr","prob":0.9586384},"annotation":["short_sentences","header","footer"],"sentence_identifications":[{"label": "fr", "prob": 0.9}]}}
{"content":"this is the main content","warc_headers":{"warc-type":"conversion","warc-date":"2021-09-16T11:37:01Z","warc-refers-to":"<urn:uuid:3cc5dbf1-6932-44e3-a5f9-87bddb242ed1>","warc-block-digest":"sha1:AAN5C7C7I2JOXM5ZYB5YNFPRC5N6GJES","content-type":"text/plain","warc-target-uri":"http://accueil-enfants-d-un-meme-pere.be/","content-length":"5095","warc-identified-content-language":"fra,eng","warc-record-id":"<urn:uuid:7c1c010a-61ca-4383-92ba-008390a56fc9>"},"metadata":{"identification":{"label":"fr","prob":0.9586384},"annotation":["short_sentences","header","footer"],"sentence_identifications":[{"label": "fr", "prob": 0.9}]}}
{"content":"this is the main content","warc_headers":{"warc-type":"conversion","warc-date":"2021-09-16T11:37:01Z","warc-refers-to":"<urn:uuid:3cc5dbf1-6932-44e3-a5f9-87bddb242ed1>","warc-block-digest":"sha1:AAN5C7C7I2JOXM5ZYB5YNFPRC5N6GJES","content-type":"text/plain","warc-target-uri":"http://accueil-enfants-d-un-meme-pere.be/","content-length":"5095","warc-identified-content-language":"fra,eng","warc-record-id":"<urn:uuid:7c1c010a-61ca-4383-92ba-008390a56fc9>"},"metadata":{"identification":{"label":"fr","prob":0.9586384},"annotation":["short_sentences","header","footer"],"sentence_identifications":[{"label": "fr", "prob": 0.9}]}}
{"content":"this is the main content","warc_headers":{"warc-type":"conversion","warc-date":"2021-09-16T11:37:01Z","warc-refers-to":"<urn:uuid:3cc5dbf1-6932-44e3-a5f9-87bddb242ed1>","warc-block-digest":"sha1:AAN5C7C7I2JOXM5ZYB5YNFPRC5N6GJES","content-type":"text/plain","warc-target-uri":"http://accueil-enfants-d-un-meme-pere.be/","content-length":"5095","warc-identified-content-language":"fra,eng","warc-record-id":"<urn:uuid:7c1c010a-61ca-4383-92ba-008390a56fc9>"},"metadata":{"identification":{"label":"fr","prob":0.9586384},"annotation":["short_sentences","header","footer"],"sentence_identifications":[{"label": "fr", "prob": 0.9}]}}"#
    }

    #[test]
    fn test_read_simple() {
        let content = get_samples();
        let mut r = DocReader::new(content.as_bytes());
        for _ in 0..5 {
            assert!(r.next().is_some());
        }
        assert!(r.next().is_none());
    }

    #[test]
    fn test_bad_format() {
        let content = r#"{"foo": "bar"}"#;
        let mut r = DocReader::new(content.as_bytes());
        match r.next() {
            Some(Err(Error::Serde(_))) => assert!(true),
            x => panic!("wrong return: {:?}", x),
        }
    }

    #[test]
    fn test_compressed_data() {
        let content = get_samples();

        //create uncompressed data
        let br = BufReader::new(content.as_bytes());
        let r = DocReader::new(br);
        let documents: Result<Vec<Document>, Error> = r.collect();

        //create compressed data
        let mut compressed_content = vec![];
        {
            let mut enc = GzEncoder::new(&mut compressed_content, Compression::fast());
            enc.write(content.as_bytes()).unwrap();
        }

        let c = Cursor::new(&mut compressed_content);
        let r = DocReader::from_gzip(c);
        let documents_from_compressed: Result<Vec<Document>, Error> = r.collect();

        assert_eq!(documents.unwrap(), documents_from_compressed.unwrap())
    }
}
