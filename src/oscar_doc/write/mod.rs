mod writer;
mod writer_parquet;
pub use writer::DocWriter;
// #[cfg(feature = "parquet")]
pub use writer_parquet::ParquetWriter;
