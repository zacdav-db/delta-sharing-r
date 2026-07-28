//! Benchmark-only Delta Kernel comparator.
//!
//! This example compiles the package's existing Kernel adapter directly. It is
//! not linked into the R package static library and adds no native entry point.

#[allow(dead_code)] // `smoke()` is part of the adapter but not this workload.
#[path = "../src/kernel/adapter.rs"]
mod adapter;

use std::env;
use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};
use std::process;
use std::time::Instant;

use adapter::SnapshotReadOptions;

#[derive(Debug, PartialEq)]
struct Config {
    table: PathBuf,
    batch_size: usize,
    repetitions: usize,
    warmups: usize,
    expected_rows: Option<u64>,
    output: PathBuf,
}

#[derive(Debug)]
struct Sample {
    iteration: usize,
    rows: u64,
    batches: usize,
    maximum_batch_rows: usize,
    construction_seconds: f64,
    first_batch_pull_seconds: f64,
    time_to_first_batch_seconds: f64,
    total_seconds: f64,
    rows_per_second: f64,
}

fn usage() -> &'static str {
    "Usage: cargo run --release --example kernel_scan_comparator -- \
     --table PATH --batch-size N --repetitions N --warmups N \
     --expected-rows N --output PATH"
}

fn parse_positive_usize(name: &str, value: &str) -> Result<usize, String> {
    let parsed = value
        .parse::<usize>()
        .map_err(|_| format!("`{name}` must be a positive integer"))?;
    if parsed == 0 {
        return Err(format!("`{name}` must be a positive integer"));
    }
    Ok(parsed)
}

fn parse_nonnegative_usize(name: &str, value: &str) -> Result<usize, String> {
    value
        .parse::<usize>()
        .map_err(|_| format!("`{name}` must be a non-negative integer"))
}

fn parse_args<I>(args: I) -> Result<Config, String>
where
    I: IntoIterator<Item = String>,
{
    let mut table = None;
    let mut batch_size = None;
    let mut repetitions = None;
    let mut warmups = None;
    let mut expected_rows = None;
    let mut output = None;
    let mut args = args.into_iter();

    while let Some(argument) = args.next() {
        let value = args
            .next()
            .ok_or_else(|| format!("argument `{argument}` requires a value"))?;
        match argument.as_str() {
            "--table" => table = Some(PathBuf::from(value)),
            "--batch-size" => {
                batch_size = Some(parse_positive_usize("--batch-size", &value)?);
            }
            "--repetitions" => {
                repetitions = Some(parse_positive_usize("--repetitions", &value)?);
            }
            "--warmups" => {
                warmups = Some(parse_nonnegative_usize("--warmups", &value)?);
            }
            "--expected-rows" => {
                expected_rows =
                    Some(value.parse::<u64>().map_err(|_| {
                        "`--expected-rows` must be a non-negative integer".to_string()
                    })?);
            }
            "--output" => output = Some(PathBuf::from(value)),
            _ => return Err(format!("unknown argument `{argument}`")),
        }
    }

    Ok(Config {
        table: table.ok_or_else(|| "`--table` is required".to_string())?,
        batch_size: batch_size.ok_or_else(|| "`--batch-size` is required".to_string())?,
        repetitions: repetitions.ok_or_else(|| "`--repetitions` is required".to_string())?,
        warmups: warmups.ok_or_else(|| "`--warmups` is required".to_string())?,
        expected_rows,
        output: output.ok_or_else(|| "`--output` is required".to_string())?,
    })
}

fn measure_once(table: &Path, batch_size: usize, iteration: usize) -> Result<Sample, String> {
    let started = Instant::now();
    let options =
        SnapshotReadOptions::try_new(table.to_string_lossy().into_owned(), None, None, batch_size)?;
    let mut reader = adapter::snapshot_reader(options)?;
    let construction_seconds = started.elapsed().as_secs_f64();

    let first_started = Instant::now();
    let first = reader
        .next()
        .transpose()
        .map_err(|error| error.to_string())?;
    let first_batch_pull_seconds = first_started.elapsed().as_secs_f64();
    let time_to_first_batch_seconds = started.elapsed().as_secs_f64();

    let mut rows = first
        .as_ref()
        .map_or(0_u64, |batch| batch.num_rows() as u64);
    let mut batches = usize::from(first.is_some());
    let mut maximum_batch_rows = first.as_ref().map_or(0, |batch| batch.num_rows());
    for batch in reader {
        let batch = batch.map_err(|error| error.to_string())?;
        rows += batch.num_rows() as u64;
        batches += 1;
        maximum_batch_rows = maximum_batch_rows.max(batch.num_rows());
    }
    let total_seconds = started.elapsed().as_secs_f64();

    Ok(Sample {
        iteration,
        rows,
        batches,
        maximum_batch_rows,
        construction_seconds,
        first_batch_pull_seconds,
        time_to_first_batch_seconds,
        total_seconds,
        rows_per_second: rows as f64 / total_seconds,
    })
}

fn json_string(value: &str) -> String {
    let mut output = String::with_capacity(value.len() + 2);
    output.push('"');
    for character in value.chars() {
        match character {
            '"' => output.push_str("\\\""),
            '\\' => output.push_str("\\\\"),
            '\n' => output.push_str("\\n"),
            '\r' => output.push_str("\\r"),
            '\t' => output.push_str("\\t"),
            character if character.is_control() => {
                write!(output, "\\u{:04x}", character as u32)
                    .expect("writing to a String cannot fail");
            }
            character => output.push(character),
        }
    }
    output.push('"');
    output
}

fn render_json(config: &Config, table: &Path, samples: &[Sample]) -> String {
    let samples = samples
        .iter()
        .map(|sample| {
            format!(
                concat!(
                    "    {{\n",
                    "      \"iteration\": {},\n",
                    "      \"rows\": {},\n",
                    "      \"batches\": {},\n",
                    "      \"maximum_batch_rows\": {},\n",
                    "      \"construction_seconds\": {:.17},\n",
                    "      \"first_batch_pull_seconds\": {:.17},\n",
                    "      \"time_to_first_batch_seconds\": {:.17},\n",
                    "      \"total_seconds\": {:.17},\n",
                    "      \"rows_per_second\": {:.17}\n",
                    "    }}"
                ),
                sample.iteration,
                sample.rows,
                sample.batches,
                sample.maximum_batch_rows,
                sample.construction_seconds,
                sample.first_batch_pull_seconds,
                sample.time_to_first_batch_seconds,
                sample.total_seconds,
                sample.rows_per_second
            )
        })
        .collect::<Vec<_>>()
        .join(",\n");

    format!(
        concat!(
            "{{\n",
            "  \"schema_version\": 1,\n",
            "  \"implementation\": \"package-kernel-adapter-direct\",\n",
            "  \"table\": {},\n",
            "  \"batch_size\": {},\n",
            "  \"repetitions\": {},\n",
            "  \"warmups\": {},\n",
            "  \"expected_rows\": {},\n",
            "  \"samples\": [\n",
            "{}\n",
            "  ]\n",
            "}}\n"
        ),
        json_string(&table.to_string_lossy()),
        config.batch_size,
        config.repetitions,
        config.warmups,
        config
            .expected_rows
            .map_or_else(|| "null".to_string(), |value| value.to_string()),
        samples
    )
}

fn write_atomically(path: &Path, contents: &str) -> Result<(), String> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)
        .map_err(|error| format!("could not create output directory: {error}"))?;
    let temporary = parent.join(format!(
        ".{}-{}.tmp",
        path.file_name()
            .and_then(|value| value.to_str())
            .unwrap_or("kernel-comparator"),
        process::id()
    ));
    fs::write(&temporary, contents)
        .map_err(|error| format!("could not write comparator output: {error}"))?;
    fs::rename(&temporary, path)
        .map_err(|error| format!("could not publish comparator output: {error}"))
}

fn run() -> Result<(), String> {
    let config = parse_args(env::args().skip(1))?;
    let table = fs::canonicalize(&config.table)
        .map_err(|error| format!("could not resolve `--table`: {error}"))?;

    for _ in 0..config.warmups {
        let sample = measure_once(&table, config.batch_size, 0)?;
        if config
            .expected_rows
            .is_some_and(|expected| sample.rows != expected)
        {
            return Err(format!(
                "warm-up returned {} rows; expected {}",
                sample.rows,
                config.expected_rows.unwrap()
            ));
        }
    }

    let mut samples = Vec::with_capacity(config.repetitions);
    for iteration in 1..=config.repetitions {
        let sample = measure_once(&table, config.batch_size, iteration)?;
        if config
            .expected_rows
            .is_some_and(|expected| sample.rows != expected)
        {
            return Err(format!(
                "iteration {iteration} returned {} rows; expected {}",
                sample.rows,
                config.expected_rows.unwrap()
            ));
        }
        samples.push(sample);
    }

    write_atomically(&config.output, &render_json(&config, &table, &samples))
}

fn main() {
    if let Err(error) = run() {
        eprintln!("kernel scan comparator: {error}");
        eprintln!("{}", usage());
        process::exit(2);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arguments_require_complete_positive_controls() {
        let parsed = parse_args([
            "--table".to_string(),
            "/tmp/table".to_string(),
            "--batch-size".to_string(),
            "65536".to_string(),
            "--repetitions".to_string(),
            "3".to_string(),
            "--warmups".to_string(),
            "1".to_string(),
            "--expected-rows".to_string(),
            "7".to_string(),
            "--output".to_string(),
            "/tmp/output.json".to_string(),
        ])
        .unwrap();
        assert_eq!(parsed.batch_size, 65_536);
        assert_eq!(parsed.repetitions, 3);
        assert_eq!(parsed.warmups, 1);
        assert_eq!(parsed.expected_rows, Some(7));
        assert!(parse_args(Vec::<String>::new()).is_err());
        assert!(parse_args([
            "--table".to_string(),
            "/tmp/table".to_string(),
            "--batch-size".to_string(),
            "0".to_string(),
        ])
        .is_err());
    }

    #[test]
    fn json_strings_escape_paths_without_serde() {
        assert_eq!(json_string("a\\b\"c\n"), "\"a\\\\b\\\"c\\n\"");
    }
}
