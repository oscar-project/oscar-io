use std::{fs::File, io::BufReader, path::PathBuf};

use oscar_io::v3::Document;
use oscar_io::{
    error::Error,
    oscar_doc::{Reader, SplitFolderReader, SplitReader},
};
#[test]
fn test_hehe() {
    let f = File::open("tests/res/data.jsonl").unwrap();
    let br = BufReader::new(&f);

    let cr = Reader::new(br);
    let docs: Vec<Result<Document, Error>> = cr.collect();

    // check that there has been no error
    assert!(docs.iter().all(Result::is_ok));

    // ensure correct length.
    let nb_docs = docs.len();
    assert_eq!(nb_docs, 63);
}

#[test]
fn test_split() {
    let f = PathBuf::from("tests/res/split/");
    let cr = SplitReader::new(f, "data_part_", "", ".jsonl", 1);
    let nb_docs = cr.into_iter().count();
    assert_eq!(nb_docs, 63);
}

#[test]
fn test_split_validity() {
    let f = File::open("tests/res/data.jsonl").unwrap();
    let br = BufReader::new(&f);
    let cr_full = Reader::new(br);

    let f = PathBuf::from("tests/res/split/");
    let cr_split = SplitReader::new(f, "data_part_", "", ".jsonl", 1);

    let docs_from_full: Vec<Result<Document, Error>> = cr_full.collect();
    assert!(docs_from_full.iter().all(Result::is_ok));
    let docs_from_full: Vec<Document> = docs_from_full.into_iter().map(|x| x.unwrap()).collect();

    let docs_from_split: Vec<Result<Document, Error>> = cr_split.collect();
    assert!(docs_from_split.iter().all(Result::is_ok));
    let docs_from_split: Vec<Document> = docs_from_split.into_iter().map(|x| x.unwrap()).collect();

    assert!(docs_from_full.len() != 0);
    assert_eq!(docs_from_full.len(), docs_from_split.len());
    assert_eq!(docs_from_full, docs_from_split);
}

#[test]
fn test_split_folder() {
    let f = PathBuf::from("tests/res/split/");
    let cr = SplitFolderReader::new(&f).unwrap();
    for d in cr {
        println!("{d:?}");
    }
    // let nb_docs = cr.into_iter().count();
    // assert_eq!(nb_docs, 63);
}
