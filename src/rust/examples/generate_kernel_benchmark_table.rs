//! Generate a deterministic, benchmark-only local Delta table.
//!
//! The executable is an example target and is never linked into the R package.

use std::env;
use std::fs::{self, File};
use std::path::PathBuf;
use std::process;
use std::sync::Arc;

use arrow_array::{ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

#[derive(Debug, PartialEq)]
struct Config {
    output: PathBuf,
    rows: usize,
    row_group_size: usize,
}

fn usage() -> &'static str {
    "Usage: cargo run --release --example generate_kernel_benchmark_table -- \
     --output PATH --rows N --row-group-size N"
}

fn parse_positive(name: &str, value: &str) -> Result<usize, String> {
    let parsed = value
        .parse::<usize>()
        .map_err(|_| format!("`{name}` must be a positive integer"))?;
    if parsed == 0 {
        return Err(format!("`{name}` must be a positive integer"));
    }
    Ok(parsed)
}

fn parse_args<I>(args: I) -> Result<Config, String>
where
    I: IntoIterator<Item = String>,
{
    let mut output = None;
    let mut rows = None;
    let mut row_group_size = None;
    let mut args = args.into_iter();
    while let Some(argument) = args.next() {
        let value = args
            .next()
            .ok_or_else(|| format!("argument `{argument}` requires a value"))?;
        match argument.as_str() {
            "--output" => output = Some(PathBuf::from(value)),
            "--rows" => rows = Some(parse_positive("--rows", &value)?),
            "--row-group-size" => {
                row_group_size = Some(parse_positive("--row-group-size", &value)?);
            }
            _ => return Err(format!("unknown argument `{argument}`")),
        }
    }
    Ok(Config {
        output: output.ok_or_else(|| "`--output` is required".to_string())?,
        rows: rows.ok_or_else(|| "`--rows` is required".to_string())?,
        row_group_size: row_group_size
            .ok_or_else(|| "`--row-group-size` is required".to_string())?,
    })
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("group", DataType::Utf8, true),
        Field::new("value", DataType::Float64, true),
        Field::new("active", DataType::Boolean, false),
    ]))
}

fn batch(schema: Arc<Schema>, start: usize, length: usize) -> Result<RecordBatch, String> {
    let end = start + length;
    let id: ArrayRef = Arc::new(Int64Array::from_iter_values(
        (start..end).map(|value| value as i64),
    ));
    let group: ArrayRef = Arc::new(StringArray::from_iter(
        (start..end).map(|value| Some(if value % 2 == 0 { "even" } else { "odd" })),
    ));
    let value: ArrayRef = Arc::new(Float64Array::from_iter(
        (start..end).map(|value| Some((value % 10_000) as f64 / 10.0)),
    ));
    let active: ArrayRef = Arc::new(BooleanArray::from_iter(
        (start..end).map(|value| Some(value % 3 != 0)),
    ));
    RecordBatch::try_new(schema, vec![id, group, value, active])
        .map_err(|error| format!("could not construct benchmark batch: {error}"))
}

fn delta_log(file_bytes: u64) -> String {
    let schema = r#"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":false,\"metadata\":{}},{\"name\":\"group\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"value\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}},{\"name\":\"active\",\"type\":\"boolean\",\"nullable\":false,\"metadata\":{}}]}"#;
    concat!(
        "{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}\n",
        "{\"metaData\":{\"id\":\"00000000-0000-0000-0000-000000000001\",",
        "\"format\":{\"provider\":\"parquet\",\"options\":{}},",
        "\"schemaString\":\"__SCHEMA__\",\"partitionColumns\":[],",
        "\"configuration\":{},\"createdTime\":0}}\n",
        "{\"add\":{\"path\":\"part-00000.parquet\",",
        "\"partitionValues\":{},\"size\":__FILE_BYTES__,",
        "\"modificationTime\":0,\"dataChange\":true}}\n"
    )
    .replace("__SCHEMA__", schema)
    .replace("__FILE_BYTES__", &file_bytes.to_string())
}

fn generate(config: &Config) -> Result<(), String> {
    if config.output.exists() {
        return Err("`--output` must not already exist".to_string());
    }
    fs::create_dir_all(config.output.join("_delta_log"))
        .map_err(|error| format!("could not create benchmark table: {error}"))?;

    let parquet_path = config.output.join("part-00000.parquet");
    let schema = schema();
    let properties = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_max_row_group_row_count(Some(config.row_group_size))
        .build();
    let file = File::create(&parquet_path)
        .map_err(|error| format!("could not create benchmark Parquet file: {error}"))?;
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(properties))
        .map_err(|error| format!("could not create benchmark Parquet writer: {error}"))?;
    let mut start = 0;
    while start < config.rows {
        let length = config.row_group_size.min(config.rows - start);
        writer
            .write(&batch(schema.clone(), start, length)?)
            .map_err(|error| format!("could not write benchmark Parquet batch: {error}"))?;
        start += length;
    }
    writer
        .close()
        .map_err(|error| format!("could not finish benchmark Parquet file: {error}"))?;

    let file_bytes = fs::metadata(&parquet_path)
        .map_err(|error| format!("could not inspect benchmark Parquet file: {error}"))?
        .len();
    fs::write(
        config
            .output
            .join("_delta_log")
            .join("00000000000000000000.json"),
        delta_log(file_bytes),
    )
    .map_err(|error| format!("could not write benchmark Delta log: {error}"))?;
    Ok(())
}

fn run() -> Result<(), String> {
    let config = parse_args(env::args().skip(1))?;
    generate(&config)
}

fn main() {
    if let Err(error) = run() {
        eprintln!("Kernel benchmark table generator: {error}");
        eprintln!("{}", usage());
        process::exit(2);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temporary_parent() -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        env::temp_dir().join(format!(
            "delta-sharing-r-generator-{}-{nanos}",
            process::id()
        ))
    }

    #[test]
    fn arguments_require_an_absent_output_and_positive_sizes() {
        let config = parse_args([
            "--output".to_string(),
            "/tmp/table".to_string(),
            "--rows".to_string(),
            "65536".to_string(),
            "--row-group-size".to_string(),
            "65536".to_string(),
        ])
        .unwrap();
        assert_eq!(config.rows, 65_536);
        assert_eq!(config.row_group_size, 65_536);
        assert!(parse_args(Vec::<String>::new()).is_err());
    }

    #[test]
    fn log_has_one_protocol_metadata_and_add_action() {
        let log = delta_log(123);
        assert_eq!(log.lines().count(), 3);
        assert!(log.contains("\"size\":123"));
        assert!(log.contains("\"schemaString\""));
    }

    #[test]
    fn generated_batch_is_deterministic() {
        let batch = batch(schema(), 10, 4).unwrap();
        assert_eq!(batch.num_rows(), 4);
        assert_eq!(batch.num_columns(), 4);
    }

    #[test]
    fn generated_tables_are_byte_identical() {
        let parent = temporary_parent();
        let first = parent.join("first");
        let second = parent.join("second");
        generate(&Config {
            output: first.clone(),
            rows: 4096,
            row_group_size: 1024,
        })
        .unwrap();
        generate(&Config {
            output: second.clone(),
            rows: 4096,
            row_group_size: 1024,
        })
        .unwrap();
        assert_eq!(
            fs::read(first.join("part-00000.parquet")).unwrap(),
            fs::read(second.join("part-00000.parquet")).unwrap()
        );
        assert_eq!(
            fs::read(first.join("_delta_log").join("00000000000000000000.json")).unwrap(),
            fs::read(second.join("_delta_log").join("00000000000000000000.json")).unwrap()
        );
        fs::remove_dir_all(parent).unwrap();
    }
}
