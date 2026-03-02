use std::path::PathBuf;

use anyhow::Result;
use clap::{Args, CommandFactory, Parser, Subcommand, ValueEnum};

use crate::core::{Marina, PullOptions, PushOptions, ResolveResult};
use crate::io::mcap_transform::{McapChunkCompression, PointCloudCompressionMode};
use crate::io::pack::ArchiveCompression;
use crate::model::bag_ref::BagRef;
use crate::progress::{ProgressReporter, WriterProgress};
use crate::storage::config::{
    self, ConfigArchiveCompression, ConfigMcapCompression, ConfigPointcloudMode, RegistryConfig,
};

#[derive(Parser)]
#[command(name = "marina")]
#[command(about = "Dataset-style ROS bag manager for MCAP bags")]
struct Cli {
    #[command(subcommand)]
    cmd: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Registry(RegistryCmd),
    #[command(alias = "ls")]
    List(LocalListArgs),
    Search(SearchArgs),
    Push(PushArgs),
    Pull(PullArgs),
    Resolve(ResolveArgs),
    Export(ExportArgs),
    Rm(RemoveArgs),
    Clean(CleanArgs),
    Complete(CompleteArgs),
    Completions(CompletionsArgs),
}

#[derive(Args)]
struct LocalListArgs {}

#[derive(Args)]
struct SearchArgs {
    pattern: String,
    #[arg(long)]
    registry: Option<String>,
}

#[derive(Subcommand)]
enum RegistrySub {
    Add(AddRegistryArgs),
    #[command(alias = "ls")]
    List,
    Rm(RemoveRegistryArgs),
}

#[derive(Args)]
struct RegistryCmd {
    #[command(subcommand)]
    cmd: RegistrySub,
}

#[derive(Args)]
struct AddRegistryArgs {
    uri: String,
    #[arg(long)]
    name: Option<String>,
    #[arg(long)]
    kind: Option<String>,
    #[arg(long)]
    auth_env: Option<String>,
}

#[derive(Args)]
struct RemoveRegistryArgs {
    name: String,
    #[arg(long)]
    delete_data: bool,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum CliPointcloudMode {
    Off,
    Lossy,
    Lossless,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum CliMcapCompression {
    None,
    Zstd,
    Lz4,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum CliArchiveCompression {
    Gzip,
    None,
}

#[derive(Args)]
struct PushArgs {
    bag: BagRef,
    source: PathBuf,
    #[arg(long)]
    registry: Option<String>,
    #[arg(long, value_enum)]
    pointcloud_mode: Option<CliPointcloudMode>,
    #[arg(long)]
    pointcloud_accuracy_mm: Option<f64>,
    #[arg(long, value_enum)]
    packed_mcap_compression: Option<CliMcapCompression>,
    #[arg(long, value_enum)]
    packed_archive_compression: Option<CliArchiveCompression>,
    #[arg(long)]
    write_http_index: bool,
    #[arg(long)]
    no_progress: bool,
}

#[derive(Args)]
struct PullArgs {
    target: String,
    #[arg(long)]
    registry: Option<String>,
    #[arg(long, value_enum)]
    unpacked_mcap_compression: Option<CliMcapCompression>,
    #[arg(long)]
    no_progress: bool,
}

#[derive(Args)]
struct ResolveArgs {
    target: String,
}

#[derive(Args)]
struct ExportArgs {
    target: BagRef,
    output: PathBuf,
}

#[derive(Args)]
struct RemoveArgs {
    bag: BagRef,
    #[arg(long)]
    remote: bool,
    #[arg(long)]
    registry: Option<String>,
}

#[derive(Args)]
struct CleanArgs {
    #[arg(short = 'a', long = "all")]
    all: bool,
}

#[derive(Args)]
struct CompleteArgs {
    prefix: String,
    #[arg(long)]
    registry: Option<String>,
}

#[derive(Args)]
struct CompletionsArgs {
    shell: clap_complete::Shell,
}

pub fn run() -> Result<()> {
    run_with_args(std::env::args().collect::<Vec<_>>())
}

pub fn run_with_args(args: Vec<String>) -> Result<()> {
    let cli = Cli::parse_from(args);
    run_parsed(cli)
}

fn run_parsed(cli: Cli) -> Result<()> {
    let compression = config::load_compression_config()?;
    let mut marina = Marina::load()?;

    match cli.cmd {
        Commands::Registry(cmd) => match cmd.cmd {
            RegistrySub::Add(args) => {
                let kind = args
                    .kind
                    .unwrap_or_else(|| infer_kind_from_uri(&args.uri).to_string());
                let name = args.name.unwrap_or_else(|| {
                    let idx = marina.list_registry_configs().len() + 1;
                    format!("{kind}-{idx}")
                });
                marina.add_registry(RegistryConfig {
                    name: name.clone(),
                    kind: kind.clone(),
                    uri: args.uri,
                    auth_env: args.auth_env,
                })?;
                println!("registry added: {} ({})", name, kind);
            }
            RegistrySub::List => {
                let registry_path = config::registry_file_path()?;
                println!("registry config: {}", registry_path.display());
                if !registry_path.exists() {
                    println!("registry file does not exist yet");
                }
                let entries = marina.list_registry_configs();
                if entries.is_empty() {
                    println!("no registries configured");
                } else {
                    for cfg in entries {
                        println!(
                            "{}\tkind={}\turi={}\tauth_env={}",
                            cfg.name,
                            cfg.kind,
                            cfg.uri,
                            cfg.auth_env.clone().unwrap_or_else(|| "-".to_string())
                        );
                    }
                }
            }
            RegistrySub::Rm(args) => {
                let removed = marina.remove_registry(&args.name, args.delete_data)?;
                println!("removed registry '{}' ({})", removed.name, removed.kind);
                if matches!(removed.kind.as_str(), "folder" | "directory") {
                    let location = local_registry_data_path(&removed.uri);
                    if removed.data_deleted {
                        println!("deleted registry data at {}", location.display());
                    } else {
                        println!(
                            "data kept at {} (use --delete-data to remove it)",
                            location.display()
                        );
                    }
                } else {
                    println!("remote location was {}", removed.uri);
                }
            }
        },
        Commands::List(_args) => {
            let items = marina.list_cached_bags();
            if items.is_empty() {
                println!("no local bagfiles cached");
            } else {
                for item in items {
                    println!("{}", item.bag);
                    println!("  path: {}", item.local_dir.display());
                    println!(
                        "  size: {} -> {}",
                        human_bytes(item.original_bytes),
                        human_bytes(item.packed_bytes)
                    );
                }
            }
        }
        Commands::Search(args) => {
            if let Some(registry) = args.registry.as_deref() {
                let out = marina.search_remote(&args.pattern, Some(registry))?;
                if out.is_empty() {
                    println!("no remote matches found in '{}'", registry);
                } else {
                    for bag in out {
                        println!("{}\t{}", registry, bag);
                    }
                }
            } else {
                let hits = marina.search_all_remotes(&args.pattern);
                if hits.is_empty() {
                    println!("no remote matches found");
                } else {
                    for hit in hits {
                        println!("{}\t{}", hit.registry, hit.bag);
                    }
                }
            }
        }
        Commands::Push(args) => {
            let push_options = PushOptions {
                pointcloud_mode: args
                    .pointcloud_mode
                    .map(cli_pointcloud_mode_to_core)
                    .unwrap_or_else(|| config_pointcloud_mode_to_core(compression.pointcloud_mode)),
                pointcloud_precision_m: args
                    .pointcloud_accuracy_mm
                    .unwrap_or(compression.pointcloud_accuracy_mm)
                    / 1000.0,
                packed_mcap_compression: args
                    .packed_mcap_compression
                    .map(cli_mcap_compression_to_core)
                    .unwrap_or_else(|| {
                        config_mcap_compression_to_core(compression.packed_mcap_compression)
                    }),
                packed_archive_compression: args
                    .packed_archive_compression
                    .map(cli_archive_compression_to_core)
                    .unwrap_or_else(|| {
                        config_archive_compression_to_core(compression.packed_archive_compression)
                    }),
                write_http_index: args.write_http_index,
            };
            if !args.no_progress {
                let mut stdout = std::io::stdout();
                let mut sink = WriterProgress::new(&mut stdout);
                let mut progress = ProgressReporter::new(&mut sink);
                marina.push_with_progress_and_options(
                    &args.bag,
                    &args.source,
                    args.registry.as_deref(),
                    push_options,
                    &mut progress,
                )?;
            } else {
                let mut progress = ProgressReporter::silent();
                marina.push_with_progress_and_options(
                    &args.bag,
                    &args.source,
                    args.registry.as_deref(),
                    push_options,
                    &mut progress,
                )?;
            }
            if let Some(stats) = marina.cached_size_stats(&args.bag) {
                print_size_summary(
                    &format!("pushed {}", args.bag.without_attachment()),
                    stats.original_bytes,
                    stats.packed_bytes,
                );
            } else {
                println!("pushed {}", args.bag.without_attachment());
            }
        }
        Commands::Pull(args) => {
            let pull_options = PullOptions {
                unpacked_mcap_compression: args
                    .unpacked_mcap_compression
                    .map(cli_mcap_compression_to_core)
                    .unwrap_or_else(|| {
                        config_mcap_compression_to_core(compression.unpacked_mcap_compression)
                    }),
            };
            if args.target.contains('*') {
                let pulled = if !args.no_progress {
                    let mut stdout = std::io::stdout();
                    let mut sink = WriterProgress::new(&mut stdout);
                    let mut progress = ProgressReporter::new(&mut sink);
                    marina.pull_pattern_with_progress_and_options(
                        &args.target,
                        args.registry.as_deref(),
                        pull_options,
                        &mut progress,
                    )?
                } else {
                    let mut progress = ProgressReporter::silent();
                    marina.pull_pattern_with_progress_and_options(
                        &args.target,
                        args.registry.as_deref(),
                        pull_options,
                        &mut progress,
                    )?
                };
                for bag in &pulled {
                    println!("pulled {}", bag);
                }
                println!("pulled {} bag(s)", pulled.len());
            } else {
                let bag: BagRef = args.target.parse()?;
                let path = if !args.no_progress {
                    let mut stdout = std::io::stdout();
                    let mut sink = WriterProgress::new(&mut stdout);
                    let mut progress = ProgressReporter::new(&mut sink);
                    marina.pull_exact_with_progress_and_options(
                        &bag,
                        args.registry.as_deref(),
                        pull_options,
                        &mut progress,
                    )?
                } else {
                    let mut progress = ProgressReporter::silent();
                    marina.pull_exact_with_progress_and_options(
                        &bag,
                        args.registry.as_deref(),
                        pull_options,
                        &mut progress,
                    )?
                };
                println!("pulled {} -> {}", bag.without_attachment(), path.display());
                if let Some(stats) = marina.cached_size_stats(&bag) {
                    print_size_summary("cached size", stats.original_bytes, stats.packed_bytes);
                }
            }
        }
        Commands::Resolve(args) => match marina.resolve_target(&args.target)? {
            ResolveResult::LocalPath(p) => println!("{}", p.display()),
            ResolveResult::Cached(p) => println!("{}", p.display()),
            ResolveResult::RemoteAvailable {
                registry,
                bag,
                needs_pull,
            } => {
                if needs_pull {
                    println!(
                        "{} available in registry '{}'; run: marina pull {} --registry {}",
                        bag, registry, bag, registry
                    );
                }
            }
        },
        Commands::Export(args) => {
            marina.export(&args.target, &args.output)?;
            println!("exported {} -> {}", args.target, args.output.display());
        }
        Commands::Rm(args) => {
            marina.remove_local(&args.bag)?;
            println!("removed local cache for {}", args.bag.without_attachment());
            if args.remote {
                marina.remove_remote(&args.bag, args.registry.as_deref())?;
                println!("removed remote {}", args.bag.without_attachment());
            }
        }
        Commands::Clean(args) => {
            marina.clean(args.all)?;
            if args.all {
                println!("removed cache and registries");
            } else {
                println!("removed cached resources (registries kept)");
            }
        }
        Commands::Complete(args) => {
            let query = format!("{}*", args.prefix);
            let out = marina.search_remote(&query, args.registry.as_deref())?;
            for bag in out {
                println!("{}", bag);
            }
        }
        Commands::Completions(args) => {
            let mut cmd = Cli::command();
            clap_complete::generate(args.shell, &mut cmd, "marina", &mut std::io::stdout());
        }
    }

    Ok(())
}

fn infer_kind_from_uri(uri: &str) -> &'static str {
    if uri.starts_with("ssh://") {
        "ssh"
    } else if uri.starts_with("gdrive://") {
        "gdrive"
    } else if uri.starts_with("http://") || uri.starts_with("https://") {
        "http"
    } else if uri.starts_with("directory://") {
        "directory"
    } else {
        "folder"
    }
}

fn cli_pointcloud_mode_to_core(mode: CliPointcloudMode) -> PointCloudCompressionMode {
    match mode {
        CliPointcloudMode::Off => PointCloudCompressionMode::Disabled,
        CliPointcloudMode::Lossy => PointCloudCompressionMode::Lossy,
        CliPointcloudMode::Lossless => PointCloudCompressionMode::Lossless,
    }
}

fn cli_mcap_compression_to_core(comp: CliMcapCompression) -> McapChunkCompression {
    match comp {
        CliMcapCompression::None => McapChunkCompression::None,
        CliMcapCompression::Zstd => McapChunkCompression::Zstd,
        CliMcapCompression::Lz4 => McapChunkCompression::Lz4,
    }
}

fn cli_archive_compression_to_core(comp: CliArchiveCompression) -> ArchiveCompression {
    match comp {
        CliArchiveCompression::Gzip => ArchiveCompression::Gzip,
        CliArchiveCompression::None => ArchiveCompression::None,
    }
}

fn config_pointcloud_mode_to_core(mode: ConfigPointcloudMode) -> PointCloudCompressionMode {
    match mode {
        ConfigPointcloudMode::Off => PointCloudCompressionMode::Disabled,
        ConfigPointcloudMode::Lossy => PointCloudCompressionMode::Lossy,
        ConfigPointcloudMode::Lossless => PointCloudCompressionMode::Lossless,
    }
}

fn config_mcap_compression_to_core(comp: ConfigMcapCompression) -> McapChunkCompression {
    match comp {
        ConfigMcapCompression::None => McapChunkCompression::None,
        ConfigMcapCompression::Zstd => McapChunkCompression::Zstd,
        ConfigMcapCompression::Lz4 => McapChunkCompression::Lz4,
    }
}

fn config_archive_compression_to_core(comp: ConfigArchiveCompression) -> ArchiveCompression {
    match comp {
        ConfigArchiveCompression::Gzip => ArchiveCompression::Gzip,
        ConfigArchiveCompression::None => ArchiveCompression::None,
    }
}

fn print_size_summary(title: &str, original_bytes: u64, packed_bytes: u64) {
    let ratio = if original_bytes > 0 {
        packed_bytes as f64 / original_bytes as f64
    } else {
        0.0
    };
    let saved = if original_bytes > packed_bytes {
        original_bytes - packed_bytes
    } else {
        0
    };
    println!("{}", title);
    println!("  original: {}", human_bytes(original_bytes));
    println!(
        "  packed:   {} ({:.1}% of original, saved {})",
        human_bytes(packed_bytes),
        ratio * 100.0,
        human_bytes(saved)
    );
}

fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0usize;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{} {}", bytes, UNITS[unit])
    } else {
        format!("{:.2} {}", value, UNITS[unit])
    }
}

fn local_registry_data_path(uri: &str) -> PathBuf {
    if let Some(rest) = uri.strip_prefix("folder://") {
        PathBuf::from(rest)
    } else if let Some(rest) = uri.strip_prefix("folder::") {
        PathBuf::from(rest)
    } else if let Some(rest) = uri.strip_prefix("directory://") {
        PathBuf::from(rest)
    } else if let Some(rest) = uri.strip_prefix("directory::") {
        PathBuf::from(rest)
    } else {
        PathBuf::from(uri)
    }
}
