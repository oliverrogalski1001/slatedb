use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

#[derive(Parser, Clone)]
#[command(name = "slatedb-ycsb")]
#[command(version)]
#[command(about = "A YCSB (Yahoo! Cloud Serving Benchmark) driver for SlateDB.")]
pub(crate) struct YcsbArgs {
    #[arg(
        short,
        long,
        help = "A .env file supplying environment variables. `CLOUD_PROVIDER` must be set."
    )]
    pub(crate) env_file: Option<String>,

    #[arg(
        short,
        long,
        help = "Path in the object store to the root directory of the SlateDB database.",
        default_value = "/slatedb-ycsb"
    )]
    pub(crate) path: String,

    #[arg(
        long,
        help = "Optional path to a SlateDB Settings TOML. Defaults to SlateDb.toml lookup."
    )]
    pub(crate) db_options_path: Option<PathBuf>,

    #[command(subcommand)]
    pub(crate) command: YcsbCommands,
}

#[derive(Subcommand, Clone)]
pub(crate) enum YcsbCommands {
    #[command(about = "Run the load phase (bulk insert recordcount records).")]
    Load(PhaseArgs),
    #[command(about = "Run the run phase (execute operationcount ops / duration).")]
    Run(PhaseArgs),
    #[command(about = "Run the load phase followed by the run phase.")]
    LoadRun(PhaseArgs),
}

#[derive(Args, Clone)]
pub(crate) struct PhaseArgs {
    #[arg(
        short = 'P',
        long = "properties",
        help = "Path to a YCSB .properties workload file."
    )]
    pub(crate) properties: PathBuf,

    #[arg(
        short = 'p',
        long = "prop",
        value_parser = parse_key_value,
        help = "Override a single property: `-p recordcount=1000`. Repeatable.",
        num_args = 0..,
    )]
    pub(crate) prop_overrides: Vec<(String, String)>,

    #[arg(
        long,
        help = "Number of concurrent worker tasks. Overrides `threadcount` in the properties file."
    )]
    pub(crate) threadcount: Option<u32>,

    #[arg(
        long,
        help = "Maximum duration in seconds to run the run phase. Ignored for `load`."
    )]
    pub(crate) duration: Option<u64>,

    #[arg(
        long,
        help = "Wait for writes to be durable before returning (flips WriteOptions::await_durable)."
    )]
    pub(crate) await_durable: bool,

    #[arg(
        long,
        help = "Master RNG seed for reproducibility.",
        default_value_t = 0xDEADBEEFCAFEBABE
    )]
    pub(crate) seed: u64,
}

fn parse_key_value(s: &str) -> Result<(String, String), String> {
    let Some(idx) = s.find('=') else {
        return Err(format!("expected key=value, got `{s}`"));
    };
    Ok((s[..idx].to_string(), s[idx + 1..].to_string()))
}
