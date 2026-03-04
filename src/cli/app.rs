use std::path::PathBuf;
use std::{io::IsTerminal, io::Write};

use anyhow::Result;
use clap::{Args, CommandFactory, Parser, Subcommand, ValueEnum};
use log::warn;

use crate::core::{Marina, PullOptions, PushOptions, ResolveResult};
use crate::io::mcap_transform::{McapChunkCompression, PointCloudCompressionMode};
use crate::io::pack::ArchiveCompression;
use crate::model::bag_ref::BagRef;
use crate::progress::{ProgressReporter, WriterProgress};
use crate::registry::driver::BagInfo;
#[cfg(feature = "gdrive")]
use crate::registry::gdrive_auth;
use crate::storage::config::{
    self, ConfigArchiveCompression, ConfigMcapCompression, ConfigPointcloudMode, RegistryConfig,
    TimeDisplay,
};

#[derive(Parser)]
#[command(name = "marina")]
#[command(about = "Dataset manager with first-class ROS bag support")]
#[command(version)]
struct Cli {
    #[arg(short = 'y', long = "yes", global = true)]
    yes: bool,
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
    /// Print the current version
    Version,
}

#[derive(Args)]
struct LocalListArgs {
    /// List datasets available in all remote registries instead of the local cache
    #[arg(long)]
    remote: bool,
    /// Filter to a specific registry (only with --remote)
    #[arg(long)]
    registry: Option<String>,
}

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
    /// Authenticate a gdrive registry via browser OAuth flow
    Auth(AuthRegistryArgs),
    /// Mirror all datasets from one registry into another
    Mirror(MirrorRegistryArgs),
}

#[derive(Args)]
struct MirrorRegistryArgs {
    /// Source registry name
    source: String,
    /// Target registry name
    target: String,
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

#[derive(Args)]
struct AuthRegistryArgs {
    /// Name of the registry to authenticate
    name: String,
    /// Show persisted OAuth status for this registry
    #[arg(long)]
    status: bool,
    /// OAuth client ID (or set MARINA_GDRIVE_CLIENT_ID env var)
    #[arg(long)]
    client_id: Option<String>,
    /// OAuth client secret (or set MARINA_GDRIVE_CLIENT_SECRET env var)
    #[arg(long)]
    client_secret: Option<String>,
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
    #[arg(value_name = "DATASET")]
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
    /// Run full packing/rewrite pipeline but do not upload or modify remote state
    #[arg(long)]
    dry_run: bool,
    /// After push, keep the ready cache and optionally delete the original source to complete a move
    #[arg(long)]
    move_to_cache: bool,
    #[arg(long)]
    no_progress: bool,
}

#[derive(Args)]
struct PullArgs {
    #[arg(value_name = "DATASET")]
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
    #[arg(value_name = "DATASET")]
    target: String,
}

#[derive(Args)]
struct ExportArgs {
    #[arg(value_name = "DATASET")]
    target: BagRef,
    output: PathBuf,
}

#[derive(Args)]
struct RemoveArgs {
    #[arg(value_name = "DATASET")]
    bag: BagRef,
    #[arg(long)]
    remote: bool,
    #[arg(long)]
    registry: Option<String>,
}

#[derive(Args)]
struct CleanArgs {
    /// Remove the added registries as well
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

/// Prompt the user to pick one registry from a list, or auto-accept the first if `yes` is set.
/// `items` is a list of `(registry_name, display_label)` pairs.
fn pick_registry(prompt: &str, items: &[(String, String)], yes: bool) -> Result<String> {
    if yes || items.len() == 1 {
        return Ok(items[0].0.clone());
    }
    eprintln!("{}:", prompt);
    for (i, (name, label)) in items.iter().enumerate() {
        eprintln!("  [{}] {}  ({})", i + 1, label, name);
    }
    eprint!("Select [1]: ");
    let mut input = String::new();
    std::io::stdin().read_line(&mut input)?;
    let input = input.trim();
    let idx: usize = if input.is_empty() {
        1
    } else {
        input
            .parse()
            .map_err(|_| anyhow::anyhow!("invalid selection"))?
    };
    if idx == 0 || idx > items.len() {
        return Err(anyhow::anyhow!("selection out of range"));
    }
    Ok(items[idx - 1].0.clone())
}

fn is_interactive_shell() -> bool {
    std::io::stdin().is_terminal() && std::io::stdout().is_terminal()
}

fn confirm_yes_default(prompt: &str, yes: bool) -> Result<bool> {
    if yes {
        return Ok(true);
    }
    eprint!("{} [Y/n]: ", prompt);
    std::io::stderr().flush()?;
    let mut input = String::new();
    std::io::stdin().read_line(&mut input)?;
    let input = input.trim().to_ascii_lowercase();
    Ok(input.is_empty() || input == "y" || input == "yes")
}

fn confirm_no_default(prompt: &str, yes: bool) -> Result<bool> {
    if yes {
        return Ok(false);
    }
    eprint!("{} [y/N]: ", prompt);
    std::io::stderr().flush()?;
    let mut input = String::new();
    std::io::stdin().read_line(&mut input)?;
    let input = input.trim().to_ascii_lowercase();
    Ok(input == "y" || input == "yes")
}

fn pick_pull_candidate_no_default(
    prompt: &str,
    items: &[(String, BagRef, Option<BagInfo>)],
    yes: bool,
    time_display: TimeDisplay,
) -> Result<Option<usize>> {
    if yes || items.is_empty() {
        return Ok(None);
    }
    eprintln!("{}:", prompt);
    eprintln!("  [0] no");
    let rows: Vec<[String; 9]> = items
        .iter()
        .enumerate()
        .map(|(i, (registry, bag, info))| {
            let (hash, orig, packed, clouds, mcap, pushed) =
                format_bag_info(info.as_ref(), time_display);
            [
                (i + 1).to_string(),
                bag.to_string(),
                registry.clone(),
                hash,
                orig,
                packed,
                clouds,
                mcap,
                pushed,
            ]
        })
        .collect();

    let headers = [
        "IDX", "DATASET", "REGISTRY", "HASH", "ORIGINAL", "PACKED", "CLOUDS", "MCAP", "PUSHED",
    ];
    let mut widths = headers.map(|h| h.len());
    for row in &rows {
        for (i, cell) in row.iter().enumerate() {
            widths[i] = widths[i].max(cell.len());
        }
    }
    let fmt_row = |cols: &[&str; 9]| {
        let mut s = String::new();
        for (i, col) in cols.iter().enumerate() {
            if i > 0 {
                s.push_str("  ");
            }
            if i + 1 < cols.len() {
                s.push_str(&format!("{:<width$}", col, width = widths[i]));
            } else {
                s.push_str(col);
            }
        }
        s
    };

    eprintln!("{}", fmt_row(&headers));
    for row in &rows {
        eprintln!("{}", fmt_row(&row.each_ref().map(|s| s.as_str())));
    }
    eprint!("Select [0]: ");
    std::io::stderr().flush()?;

    let mut input = String::new();
    std::io::stdin().read_line(&mut input)?;
    let input = input.trim().to_ascii_lowercase();
    if input.is_empty() || input == "0" || input == "n" || input == "no" {
        return Ok(None);
    }

    let idx: usize = input
        .parse()
        .map_err(|_| anyhow::anyhow!("invalid selection"))?;
    if idx == 0 || idx > items.len() {
        return Err(anyhow::anyhow!("selection out of range"));
    }
    Ok(Some(idx - 1))
}

fn print_remote_detail_table(
    all: Vec<(String, BagRef, Option<BagInfo>)>,
    time_display: TimeDisplay,
) {
    if all.is_empty() {
        println!("no remote datasets found");
        return;
    }

    let rows: Vec<[String; 8]> = all
        .into_iter()
        .map(|(registry, bag, info)| {
            let (hash, orig, packed, clouds, mcap, pushed) =
                format_bag_info(info.as_ref(), time_display);
            [
                bag.to_string(),
                registry,
                hash,
                orig,
                packed,
                clouds,
                mcap,
                pushed,
            ]
        })
        .collect();
    let headers = [
        "DATASET", "REGISTRY", "HASH", "ORIGINAL", "PACKED", "CLOUDS", "MCAP", "PUSHED",
    ];
    let mut widths = headers.map(|h| h.len());
    for row in &rows {
        for (i, cell) in row.iter().enumerate() {
            widths[i] = widths[i].max(cell.len());
        }
    }
    let fmt_row = |cols: &[&str; 8]| {
        let mut s = String::new();
        for (i, col) in cols.iter().enumerate() {
            if i > 0 {
                s.push_str("  ");
            }
            if i + 1 < cols.len() {
                s.push_str(&format!("{:<width$}", col, width = widths[i]));
            } else {
                s.push_str(col);
            }
        }
        s
    };
    println!("{}", fmt_row(&headers));
    for row in &rows {
        println!("{}", fmt_row(&row.each_ref().map(|s| s.as_str())));
    }
}

fn pull_and_print(
    marina: &mut Marina,
    bag: &BagRef,
    registry: Option<&str>,
    pull_options: PullOptions,
) -> Result<()> {
    let path = {
        let mut stdout = std::io::stdout();
        let mut sink = WriterProgress::new(&mut stdout);
        let mut progress = ProgressReporter::new(&mut sink);
        marina.pull_exact_with_progress_and_options(bag, registry, pull_options, &mut progress)?
    };
    println!("pulled {} -> {}", bag.without_attachment(), path.display());
    if let Some(stats) = marina.cached_size_stats(bag) {
        print_size_summary("cached size", stats.original_bytes, stats.packed_bytes);
    }
    Ok(())
}

fn run_parsed(cli: Cli) -> Result<()> {
    let yes = cli.yes;
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
            RegistrySub::Auth(args) => {
                let cfg = marina
                    .list_registry_configs()
                    .into_iter()
                    .find(|c| c.name == args.name)
                    .ok_or_else(|| anyhow::anyhow!("registry '{}' not found", args.name))?
                    .clone();
                if cfg.kind != "gdrive" {
                    return Err(anyhow::anyhow!(
                        "registry '{}' is kind '{}', only gdrive registries support OAuth auth",
                        args.name,
                        cfg.kind
                    ));
                }
                #[cfg(not(feature = "gdrive"))]
                {
                    let _ = args;
                    return Err(anyhow::anyhow!(
                        "gdrive support is disabled in this build; rebuild with feature `gdrive`"
                    ));
                }
                #[cfg(feature = "gdrive")]
                {
                    if args.status {
                        let status = gdrive_auth::oauth_status(&args.name)?;
                        println!("registry: {}", args.name);
                        println!("token file: {}", status.token_path.display());
                        if !status.token_present {
                            println!("status: missing (run `marina registry auth {}`)", args.name);
                        } else if status.token_valid {
                            println!("status: valid");
                            if let Some(expires_at) = status.expires_at {
                                let now = std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .map(|d| d.as_secs())
                                    .unwrap_or(0);
                                let remaining = expires_at.saturating_sub(now);
                                println!("expires in: {}s", remaining);
                            }
                        } else {
                            println!("status: invalid (refresh failed)");
                            if let Some(err) = status.refresh_error {
                                println!("refresh error: {}", err);
                            }
                        }
                        return Ok(());
                    }
                    let (client_id, client_secret) = gdrive_auth::resolve_client_credentials(
                        args.client_id,
                        args.client_secret,
                    )?;
                    gdrive_auth::run_oauth_flow(&args.name, &client_id, &client_secret)?;
                }
            }
            RegistrySub::Mirror(args) => {
                let mut out = std::io::stdout();
                let mut sink = WriterProgress::new(&mut out);
                let mut progress = ProgressReporter::new(&mut sink);
                let stats = marina.mirror_registry(&args.source, &args.target, &mut progress)?;
                println!(
                    "mirror complete: {} pushed, {} updated, {} skipped",
                    stats.pushed, stats.updated, stats.skipped
                );
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
        Commands::List(args) => {
            if args.remote {
                let all = if let Some(reg) = args.registry.as_deref() {
                    marina
                        .search_remote_with_info(reg, "*")
                        .into_iter()
                        .map(|(bag, info)| (reg.to_string(), bag, info))
                        .collect::<Vec<_>>()
                } else {
                    let mut rows = Vec::new();
                    for cfg in marina.list_registry_configs() {
                        match marina.search_remote("*", Some(&cfg.name)) {
                            Ok(bags) => {
                                rows.extend(bags.into_iter().map(|bag| {
                                    let info = marina.bag_info(&cfg.name, &bag);
                                    (cfg.name.clone(), bag, info)
                                }));
                            }
                            Err(err) => {
                                warn!("failed listing remote registry '{}': {}", cfg.name, err);
                            }
                        }
                    }
                    rows
                };
                let time_display = config::load_registries()
                    .map(|f| f.settings.time_display)
                    .unwrap_or_default();
                print_remote_detail_table(all, time_display);
            } else {
                let items = marina.list_cached_bags();
                if items.is_empty() {
                    println!("no local datasets cached");
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
        }
        Commands::Search(args) => {
            let rows = if let Some(registry) = args.registry.as_deref() {
                marina
                    .search_remote_with_info(registry, &args.pattern)
                    .into_iter()
                    .map(|(bag, info)| (registry.to_string(), bag, info))
                    .collect::<Vec<_>>()
            } else {
                let mut rows = Vec::new();
                for cfg in marina.list_registry_configs() {
                    match marina.search_remote(&args.pattern, Some(&cfg.name)) {
                        Ok(bags) => {
                            rows.extend(bags.into_iter().map(|bag| {
                                let info = marina.bag_info(&cfg.name, &bag);
                                (cfg.name.clone(), bag, info)
                            }));
                        }
                        Err(err) => {
                            warn!("failed searching registry '{}': {}", cfg.name, err);
                        }
                    }
                }
                rows
            };
            let time_display = config::load_registries()
                .map(|f| f.settings.time_display)
                .unwrap_or_default();
            print_remote_detail_table(rows, time_display);
        }
        Commands::Push(args) => {
            let registry = match args.registry.clone() {
                Some(r) => Some(r),
                None => {
                    let cfgs = marina.list_registry_configs();
                    if cfgs.len() > 1 {
                        let items: Vec<(String, String)> = cfgs
                            .iter()
                            .map(|c| (c.name.clone(), format!("{} {}", c.kind, c.uri)))
                            .collect();
                        Some(pick_registry(
                            "Multiple registries configured, pick one to push to",
                            &items,
                            yes,
                        )?)
                    } else {
                        None
                    }
                }
            };
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
                dry_run: args.dry_run,
            };
            if !args.no_progress {
                let mut stdout = std::io::stdout();
                let mut sink = WriterProgress::new(&mut stdout);
                let mut progress = ProgressReporter::new(&mut sink);
                marina.push_with_progress_and_options(
                    &args.bag,
                    &args.source,
                    registry.as_deref(),
                    push_options,
                    &mut progress,
                )?;
            } else {
                let mut progress = ProgressReporter::silent();
                marina.push_with_progress_and_options(
                    &args.bag,
                    &args.source,
                    registry.as_deref(),
                    push_options,
                    &mut progress,
                )?;
            }
            if args.dry_run {
                println!(
                    "dry-run complete for {} (no upload performed)",
                    args.bag.without_attachment()
                );
            } else if let Some(stats) = marina.cached_size_stats(&args.bag) {
                print_size_summary(
                    &format!("pushed {}", args.bag.without_attachment()),
                    stats.original_bytes,
                    stats.packed_bytes,
                );
            } else {
                println!("pushed {}", args.bag.without_attachment());
            }

            if args.move_to_cache && !args.dry_run && is_interactive_shell() {
                if confirm_no_default(
                    &format!(
                        "Delete source '{}' now? (cache is already populated)",
                        args.source.display()
                    ),
                    yes,
                )? {
                    if args.source.is_dir() {
                        std::fs::remove_dir_all(&args.source)?;
                    } else if args.source.is_file() {
                        std::fs::remove_file(&args.source)?;
                    }
                    println!("deleted source {}", args.source.display());
                }
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
            // Resolve which registry to use, disambiguating if needed
            let registry: Option<String> = match args.registry.clone() {
                Some(r) => Some(r),
                None => {
                    let mut unique: Vec<(String, String)> = Vec::new();
                    for cfg in marina.list_registry_configs() {
                        match marina.search_remote(&args.target, Some(&cfg.name)) {
                            Ok(bags) => {
                                if let Some(bag) = bags.first() {
                                    let hash = marina
                                        .bag_info(&cfg.name, bag)
                                        .and_then(|i| i.bundle_hash)
                                        .map(|hx| format!("hash:{hx}"))
                                        .unwrap_or_default();
                                    unique
                                        .push((cfg.name.clone(), format!("{} {}", cfg.name, hash)));
                                }
                            }
                            Err(err) => {
                                warn!("failed searching registry '{}': {}", cfg.name, err);
                            }
                        }
                    }
                    unique.sort_by(|a, b| a.0.cmp(&b.0));
                    if unique.len() > 1 {
                        Some(pick_registry(
                            &format!("'{}' found in multiple registries, pick one", args.target),
                            &unique,
                            yes,
                        )?)
                    } else if unique.len() == 1 {
                        Some(unique[0].0.clone())
                    } else {
                        None
                    }
                }
            };
            if args.target.contains('*') {
                let pulled = if !args.no_progress {
                    let mut stdout = std::io::stdout();
                    let mut sink = WriterProgress::new(&mut stdout);
                    let mut progress = ProgressReporter::new(&mut sink);
                    marina.pull_pattern_with_progress_and_options(
                        &args.target,
                        registry.as_deref(),
                        pull_options,
                        &mut progress,
                    )?
                } else {
                    let mut progress = ProgressReporter::silent();
                    marina.pull_pattern_with_progress_and_options(
                        &args.target,
                        registry.as_deref(),
                        pull_options,
                        &mut progress,
                    )?
                };
                for bag in &pulled {
                    println!("pulled {}", bag);
                }
                println!("pulled {} dataset(s)", pulled.len());
            } else {
                let bag: BagRef = args.target.parse()?;
                if args.no_progress {
                    let mut progress = ProgressReporter::silent();
                    let path = marina.pull_exact_with_progress_and_options(
                        &bag,
                        registry.as_deref(),
                        pull_options,
                        &mut progress,
                    )?;
                    println!("pulled {} -> {}", bag.without_attachment(), path.display());
                    if let Some(stats) = marina.cached_size_stats(&bag) {
                        print_size_summary("cached size", stats.original_bytes, stats.packed_bytes);
                    }
                } else {
                    pull_and_print(&mut marina, &bag, registry.as_deref(), pull_options)?;
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
                    if is_interactive_shell() {
                        println!("{} available in registry '{}'", bag, registry);
                    } else {
                        println!(
                            "{} available in registry '{}'; run: marina pull {} --registry {}",
                            bag, registry, bag, registry
                        );
                    }
                    if is_interactive_shell()
                        && confirm_yes_default(
                            &format!("pull {} from '{}' now?", bag.without_attachment(), registry),
                            yes,
                        )?
                    {
                        let pull_options = PullOptions {
                            unpacked_mcap_compression: config_mcap_compression_to_core(
                                compression.unpacked_mcap_compression,
                            ),
                        };
                        let path = {
                            let mut stdout = std::io::stdout();
                            let mut sink = WriterProgress::new(&mut stdout);
                            let mut progress = ProgressReporter::new(&mut sink);
                            marina.pull_exact_with_progress_and_options(
                                &bag,
                                Some(registry.as_str()),
                                pull_options,
                                &mut progress,
                            )?
                        };
                        println!("pulled {} -> {}", bag.without_attachment(), path.display());
                        if let Some(stats) = marina.cached_size_stats(&bag) {
                            print_size_summary(
                                "cached size",
                                stats.original_bytes,
                                stats.packed_bytes,
                            );
                        }
                    }
                }
            }
            ResolveResult::Ambiguous { candidates } => {
                if is_interactive_shell() {
                    let items: Vec<(String, BagRef, Option<BagInfo>)> = candidates
                        .iter()
                        .map(|(registry, bag)| {
                            let info = marina.bag_info(registry, bag);
                            (registry.clone(), bag.clone(), info)
                        })
                        .collect();

                    let time_display = config::load_registries()
                        .map(|f| f.settings.time_display)
                        .unwrap_or_default();

                    if let Some(choice) = pick_pull_candidate_no_default(
                        &format!("'{}' found in multiple registries, pull now?", args.target),
                        &items,
                        yes,
                        time_display,
                    )? {
                        let (registry, bag, _) = &items[choice];
                        let pull_options = PullOptions {
                            unpacked_mcap_compression: config_mcap_compression_to_core(
                                compression.unpacked_mcap_compression,
                            ),
                        };
                        pull_and_print(&mut marina, bag, Some(registry.as_str()), pull_options)?;
                    }
                } else {
                    println!("'{}' found in multiple registries:", args.target);
                    for (registry, bag) in &candidates {
                        let hash = marina
                            .bag_info(registry, bag)
                            .and_then(|i| i.bundle_hash)
                            .map(|h| format!("  [hash:{h}]"))
                            .unwrap_or_default();
                        println!("  marina pull {} --registry {}{}", bag, registry, hash);
                    }
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
        Commands::Version => {
            println!("{}", env!("CARGO_PKG_VERSION"));
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
    let saved = original_bytes.saturating_sub(packed_bytes);
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

fn format_pushed_at(pushed_at: Option<u64>, display: TimeDisplay) -> String {
    let Some(ts) = pushed_at else {
        return "-".into();
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    if display == TimeDisplay::Absolute {
        // Format as YYYY-MM-DD using only the timestamp
        let secs_per_day = 86400u64;
        let days_since_epoch = ts / secs_per_day;
        // Compute Gregorian date from days since 1970-01-01
        let mut y = 1970u32;
        let mut d = days_since_epoch as u32;
        loop {
            let days_in_year = if y % 4 == 0 && (y % 100 != 0 || y % 400 == 0) {
                366
            } else {
                365
            };
            if d < days_in_year {
                break;
            }
            d -= days_in_year;
            y += 1;
        }
        let leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
        let month_days = [
            31u32,
            if leap { 29 } else { 28 },
            31,
            30,
            31,
            30,
            31,
            31,
            30,
            31,
            30,
            31,
        ];
        let mut m = 1u32;
        for md in &month_days {
            if d < *md {
                break;
            }
            d -= md;
            m += 1;
        }
        return format!("{:04}-{:02}-{:02}", y, m, d + 1);
    }

    let elapsed = now.saturating_sub(ts);
    match elapsed {
        0..=59 => format!("{}s ago", elapsed),
        60..=3599 => format!("{}m ago", elapsed / 60),
        3600..=86399 => format!("{}h ago", elapsed / 3600),
        86400..=604799 => format!("{}d ago", elapsed / 86400),
        604800..=2591999 => format!("{}w ago", elapsed / 604800),
        2592000..=31535999 => format!("{}mo ago", elapsed / 2592000),
        _ => format!("{}y ago", elapsed / 31536000),
    }
}

fn format_bag_info(
    info: Option<&BagInfo>,
    time_display: TimeDisplay,
) -> (String, String, String, String, String, String) {
    match info {
        None => (
            "-".into(),
            "-".into(),
            "-".into(),
            "-".into(),
            "-".into(),
            "-".into(),
        ),
        Some(i) => (
            i.bundle_hash.clone().unwrap_or_else(|| "-".into()),
            human_bytes(i.original_bytes),
            human_bytes(i.packed_bytes),
            i.pointcloud.clone().unwrap_or_else(|| "-".into()),
            i.mcap_compression.clone().unwrap_or_else(|| "-".into()),
            format_pushed_at(i.pushed_at, time_display),
        ),
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
