use std::{fs::File, io::BufReader, path::PathBuf};

use oscar_io::oscar_doc::{Reader, SplitReader};
#[test]
fn test_hehe() {
    let f = File::open("tests/res/data.jsonl").unwrap();
    let br = BufReader::new(&f);

    let cr = Reader::new(br);

    let nb_docs = cr.into_iter().count();
    assert_eq!(nb_docs, 100);
}

#[test]
fn test_split() {
    // let f = File::open("tests/res/split/").unwrap();
    // let br = BufReader::new(&f);

    let f = PathBuf::from("tests/res/split/");
    let cr = SplitReader::new(f, "data_part_", "", ".jsonl", 1);

    let nb_docs = cr.into_iter().count();
    assert_eq!(nb_docs, 100);
}
