use std::{
    borrow::Cow,
    fs::File,
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    str::FromStr,
};

use crate::Error;

pub enum Comp {
    Zstd { level: i32 },
}

impl Comp {
    pub fn extension(&self) -> &str {
        match &self {
            Self::Zstd { level: _ } => "zstd",
        }
    }
}

pub struct NewWriter {
    dir: PathBuf,
    file_stem: String,

    comp: Option<Comp>,
    size_b: usize,
    max_size_b: Option<usize>,

    writer: Box<dyn Write + Send>,
    nb_files: u64,
}

impl NewWriter {
    pub fn new(
        dir: &Path,
        file_stem: String,
        comp: Option<Comp>,
        max_size_b: Option<usize>,
    ) -> Result<Self, Error> {
        let filepath = Self::assemble_filepath(dir, &file_stem, comp.as_ref());

        let writer = Self::new_writer(&filepath, comp.as_ref())?;

        Ok(Self {
            dir: dir.to_path_buf(),
            file_stem,
            comp,
            writer,
            size_b: 0,
            max_size_b,
            nb_files: 1,
        })
    }

    #[inline]

    /// Assembles a file path from a base directory, a file stem (without extensions), and a compression.
    fn assemble_filepath(dir: &Path, file_stem: &str, comp: Option<&Comp>) -> PathBuf {
        if dir.is_file() {
            dir.to_path_buf()
        } else {
            let mut path = dir.to_path_buf();
            let extension: Cow<str> = if let Some(c) = comp {
                format!("jsonl.{}", c.extension()).into()
            } else {
                "jsonl".into()
            };
            path.push(format!("{file_stem}.{extension}"));
            path
        }
    }

    #[inline]
    /// Gets current filepath.
    fn current_filepath(&self) -> PathBuf {
        // TODO simplify. Repetition because of ownership issues.
        if self.nb_files == 1 {
            Self::assemble_filepath(&self.dir, &self.file_stem, self.comp.as_ref())
        } else {
            let filestem = format!("{}_part_{}", self.file_stem, self.nb_files);
            Self::assemble_filepath(&self.dir, &filestem, self.comp.as_ref())
        }
    }

    /// Doesn't change state
    ///
    /// Also returns the stem itself
    /// if current is foo.jsonl, will return foo_part_2.jsonl (so you have to rename the first one yourself)
    /// if current is foo_part_n.json, will return foo_part_n+1.json
    #[inline]
    fn next_filepath(&self) -> PathBuf {
        let new_file_stem = if self.nb_files == 1 {
            let mut file_stem = self.file_stem.clone();
            file_stem.push_str("_part_2");
            file_stem
        } else {
            let mut file_stem = self.file_stem.clone();
            file_stem.push_str(&format!("_part_{}", self.nb_files + 1));
            file_stem
        };

        Self::assemble_filepath(&self.dir, &new_file_stem, self.comp.as_ref())
    }

    /// Rotates file
    fn rotate_file(&mut self) -> Result<(), std::io::Error> {
        let current_filename = Self::current_filepath(self);
        let next_filename = Self::next_filepath(self);

        // early return if filename exists
        if next_filename.exists() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!("{:?}", next_filename),
            ));
        }
        // if we're at first file, rename to part_1.
        if self.nb_files == 1 {
            // fix part_1
            let mut file_stem = self.file_stem.clone();
            file_stem.push_str("_part_1");

            // holds foo_part_1.jsonl
            let fixed_first_fp = Self::assemble_filepath(&self.dir, &file_stem, self.comp.as_ref());

            self.nb_files += 1;

            // close before renaming
            // There's some DRY here because we want to drop the writer *before* renaming the file
            // It could be okay to keep the renaming here, and remove the else clause to drop + create new writer afterwards
            //self.writer.flush()?;
            self.writer = Self::new_writer(&next_filename, self.comp.as_ref())?;

            std::fs::rename(current_filename, fixed_first_fp)?;

        // close part_n, open part_n+1
        } else {
            //self.writer.flush()?;
            self.writer = Self::new_writer(&next_filename, self.comp.as_ref())?;
            self.nb_files += 1;

            return Ok(());
        }

        self.size_b = 0;

        Ok(())
    }

    fn new_writer(fp: &Path, comp: Option<&Comp>) -> Result<Box<dyn Write + Send>, std::io::Error> {
        let f = File::create(fp)?;

        // Create writer depending on comp
        let writer: Box<dyn Write + Send> = match comp {
            None => Box::new(BufWriter::new(f)),
            Some(Comp::Zstd { level }) => {
                Box::new(zstd::Encoder::new(f, *level).unwrap().auto_finish())
            }
        };

        Ok(writer)
    }
}

impl Write for NewWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let bytes_written = if let Some(max_size_b) = self.max_size_b {
            // check if there's enough space to write.
            // There's an edge case here, if we're at the first part AND buf size is greater than max size b.
            // The other part of the if covers that by looking if size_b is 0. If it is, write anyway.
            if (self.size_b + (buf.len()) < max_size_b) || self.size_b == 0 {
                let bw = self.writer.write(buf)?;
                self.size_b += bw;

                bw
            } else {
                self.rotate_file()?;
                //TODO add log if bufsize > max_size
                self.size_b = self.writer.write(buf)?;

                self.size_b
            }

            // if not, rotate file and write.
            // If the len(buf) > max_size, we write anyway.
            // TODO: add warning?
        } else {
            self.writer.write(buf)?
        };

        Ok(bytes_written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }
}

#[cfg(test)]

mod test {
    use std::{fs::File, io::Write, thread, time};

    use tempfile::tempdir;

    use crate::v3::writer::new_writer::Comp;

    use super::NewWriter;

    #[test]
    fn test_unbound_uncompressed() {
        let dir = tempdir().unwrap();
        let stem = "test".to_string();
        let mut w = NewWriter::new(dir.path(), stem, None, None).unwrap();
        let data = vec!["test\n", "data\n", ":)\n"];

        for d in &data {
            w.write_all(d.as_bytes()).unwrap();
        }
        w.flush().unwrap();

        let res = std::fs::read_to_string(w.current_filepath()).unwrap();

        assert_eq!(data.join(""), res);
    }

    #[test]
    // We bound each file to 5 bytes of data
    // We should have the following result:
    // 1: test\n
    // 2: 1\n2\n
    // 3: data\n
    // 4: :)\n
    fn test_bound_uncompressed() {
        let dir = tempdir().unwrap();
        let stem = "test".to_string();
        let bound = 5;
        let mut w = NewWriter::new(dir.path(), stem, None, Some(bound)).unwrap();

        let data = vec!["test\n", "1\n", "2\n", "data\n", ":)\n"];
        let expected = vec!["test\n", "1\n2\n", "data\n", ":)\n"];

        // write data
        for d in &data {
            w.write_all(d.as_bytes()).unwrap();
        }
        w.flush().unwrap();

        for idx in 1..=4 {
            let mut p = dir.path().to_owned();
            p.push(format!("test_part_{idx}.jsonl"));
            let res = std::fs::read_to_string(&p).unwrap();
            assert_eq!(res, expected[idx - 1]);
        }
    }

    #[test]
    fn test_unbound_compressed() {
        let dir = tempdir().unwrap();
        let stem = "test".to_string();
        let mut w = NewWriter::new(dir.path(), stem, Some(Comp::Zstd { level: 0 }), None).unwrap();
        let data = vec!["test\n", "data\n", ":)\n"];

        for d in &data {
            w.write_all(d.as_bytes()).unwrap();
        }
        w.flush().unwrap();

        let p = w.current_filepath();
        std::mem::drop(w);

        let f = File::open(p).unwrap();
        let dec = zstd::decode_all(f).unwrap();
        let res = String::from_utf8(dec).unwrap();

        assert_eq!(res, data.join(""))
    }

    #[test]
    // We bound each file to 5 bytes of data
    // We should have the following result:
    // 1: test\n
    // 2: 1\n2\n
    // 3: data\n
    // 4: :)\n
    fn test_bound_compressed() {
        let dir = tempdir().unwrap();
        let stem = "test".to_string();
        let bound = 5;
        let mut w =
            NewWriter::new(dir.path(), stem, Some(Comp::Zstd { level: 0 }), Some(bound)).unwrap();

        let data = vec!["test\n", "1\n", "2\n", "data\n", ":)\n"];
        let expected = vec!["test\n", "1\n2\n", "data\n", ":)\n"];

        // write data
        for d in &data {
            w.write_all(d.as_bytes()).unwrap();
        }
        w.flush().unwrap();

        // needed for W to call finish on zstd encoder.
        std::mem::drop(w);

        for idx in 1..=4 {
            let mut p = dir.path().to_owned();
            p.push(format!("test_part_{idx}.jsonl.zstd"));
            let f = File::open(p).unwrap();
            let dec = zstd::decode_all(f).unwrap();
            let res = String::from_utf8(dec).unwrap();
            assert_eq!(res, expected[idx - 1]);
        }
    }
}
