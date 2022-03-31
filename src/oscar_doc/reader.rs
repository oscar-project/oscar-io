/*! OSCAR Schema v2 (22.01) Reader.

   Provides a way to read [Document]s from a [BufRead].

   TODO: Find a way to provide some reading of splitted corpora.
* !*/
use std::io::BufRead;

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

#[cfg(test)]
mod tests {

    use crate::error::Error;

    use super::DocReader;

    #[test]
    fn test_read_simple() {
        let content = r#"{"content":"this is the main content","warc_headers":{"warc-type":"conversion","warc-date":"2021-09-16T11:37:01Z","warc-refers-to":"<urn:uuid:3cc5dbf1-6932-44e3-a5f9-87bddb242ed1>","warc-block-digest":"sha1:AAN5C7C7I2JOXM5ZYB5YNFPRC5N6GJES","content-type":"text/plain","warc-target-uri":"http://accueil-enfants-d-un-meme-pere.be/","content-length":"5095","warc-identified-content-language":"fra,eng","warc-record-id":"<urn:uuid:7c1c010a-61ca-4383-92ba-008390a56fc9>"},"metadata":{"identification":{"label":"fr","prob":0.9586384},"annotation":["short_sentences","header","footer"],"sentence_identifications":[{"label": "fr", "prob": 0.9}]}}
{"content":"this is the main content","warc_headers":{"warc-type":"conversion","warc-date":"2021-09-16T11:37:01Z","warc-refers-to":"<urn:uuid:3cc5dbf1-6932-44e3-a5f9-87bddb242ed1>","warc-block-digest":"sha1:AAN5C7C7I2JOXM5ZYB5YNFPRC5N6GJES","content-type":"text/plain","warc-target-uri":"http://accueil-enfants-d-un-meme-pere.be/","content-length":"5095","warc-identified-content-language":"fra,eng","warc-record-id":"<urn:uuid:7c1c010a-61ca-4383-92ba-008390a56fc9>"},"metadata":{"identification":{"label":"fr","prob":0.9586384},"annotation":["short_sentences","header","footer"],"sentence_identifications":[{"label": "fr", "prob": 0.9}]}}
{"content":"this is the main content","warc_headers":{"warc-type":"conversion","warc-date":"2021-09-16T11:37:01Z","warc-refers-to":"<urn:uuid:3cc5dbf1-6932-44e3-a5f9-87bddb242ed1>","warc-block-digest":"sha1:AAN5C7C7I2JOXM5ZYB5YNFPRC5N6GJES","content-type":"text/plain","warc-target-uri":"http://accueil-enfants-d-un-meme-pere.be/","content-length":"5095","warc-identified-content-language":"fra,eng","warc-record-id":"<urn:uuid:7c1c010a-61ca-4383-92ba-008390a56fc9>"},"metadata":{"identification":{"label":"fr","prob":0.9586384},"annotation":["short_sentences","header","footer"],"sentence_identifications":[{"label": "fr", "prob": 0.9}]}}
{"content":"this is the main content","warc_headers":{"warc-type":"conversion","warc-date":"2021-09-16T11:37:01Z","warc-refers-to":"<urn:uuid:3cc5dbf1-6932-44e3-a5f9-87bddb242ed1>","warc-block-digest":"sha1:AAN5C7C7I2JOXM5ZYB5YNFPRC5N6GJES","content-type":"text/plain","warc-target-uri":"http://accueil-enfants-d-un-meme-pere.be/","content-length":"5095","warc-identified-content-language":"fra,eng","warc-record-id":"<urn:uuid:7c1c010a-61ca-4383-92ba-008390a56fc9>"},"metadata":{"identification":{"label":"fr","prob":0.9586384},"annotation":["short_sentences","header","footer"],"sentence_identifications":[{"label": "fr", "prob": 0.9}]}}
{"content":"this is the main content","warc_headers":{"warc-type":"conversion","warc-date":"2021-09-16T11:37:01Z","warc-refers-to":"<urn:uuid:3cc5dbf1-6932-44e3-a5f9-87bddb242ed1>","warc-block-digest":"sha1:AAN5C7C7I2JOXM5ZYB5YNFPRC5N6GJES","content-type":"text/plain","warc-target-uri":"http://accueil-enfants-d-un-meme-pere.be/","content-length":"5095","warc-identified-content-language":"fra,eng","warc-record-id":"<urn:uuid:7c1c010a-61ca-4383-92ba-008390a56fc9>"},"metadata":{"identification":{"label":"fr","prob":0.9586384},"annotation":["short_sentences","header","footer"],"sentence_identifications":[{"label": "fr", "prob": 0.9}]}}"#;
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
}
