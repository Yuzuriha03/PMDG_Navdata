use anyhow::{bail, Context, Result};
use clap::{Parser, ValueEnum};
use configparser::ini::Ini;
use log::info;
mod airports;
mod core;
mod enroute;
mod terminal;

use crate::{
    airports::{airport_data::process_airports_to_db, runways::process_runways_to_db},
    core::{
        db::{
            close_shared_connection, get_shared_connection, open_sqlite_connection,
            quote_sqlite_identifier, set_shared_connection, RustSqliteConnection,
        },
        logging::init_cli_logging,
    },
    enroute::{
        airways::process_airways_to_db, epoints::process_enroute_waypoints_to_db,
        gs::process_ils_gs_to_db, ndbs::process_ndbs_to_db, vhfs::process_vhfs_to_db,
    },
    terminal::{
        procedures::{process_terminal_cifp_to_db, TerminalProcedureConfig},
        tpoints::process_terminal_waypoints_file_to_db,
    },
};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;
use std::time::Instant;

const DEFAULT_SID_PREFIXES: &[&str] = &[
    "OP", "ZB", "ZY", "ZS", "ZG", "ZL", "ZH", "ZU", "ZP", "ZW", "ZJ", "ZZ",
];
const DEFAULT_TERMINAL_PREFIXES: &[&str] = &[
    "OP", "VH", "ZB", "ZY", "ZS", "ZG", "ZL", "ZH", "ZU", "ZP", "ZW", "ZJ", "ZZ",
];

struct TerminalCifpStepConfig {
    table_name: &'static str,
    procedure_prefix: &'static str,
    airport_col_idx: usize,
    procedure_col_idx: usize,
    prefixes: &'static [&'static str],
    is_sid: bool,
    is_iap: bool,
}

const SIDS_STEP_CONFIG: TerminalCifpStepConfig = TerminalCifpStepConfig {
    table_name: "tbl_sids",
    procedure_prefix: "SID:",
    airport_col_idx: 4,
    procedure_col_idx: 7,
    prefixes: DEFAULT_SID_PREFIXES,
    is_sid: true,
    is_iap: false,
};

const STARS_STEP_CONFIG: TerminalCifpStepConfig = TerminalCifpStepConfig {
    table_name: "tbl_stars",
    procedure_prefix: "STAR:",
    airport_col_idx: 5,
    procedure_col_idx: 8,
    prefixes: DEFAULT_TERMINAL_PREFIXES,
    is_sid: false,
    is_iap: false,
};

const IAPS_STEP_CONFIG: TerminalCifpStepConfig = TerminalCifpStepConfig {
    table_name: "tbl_iaps",
    procedure_prefix: "APPCH:",
    airport_col_idx: 6,
    procedure_col_idx: 9,
    prefixes: DEFAULT_TERMINAL_PREFIXES,
    is_sid: false,
    is_iap: true,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum Step {
    All,
    Airports,
    Runways,
    Vhfs,
    Gs,
    Ndbs,
    EnrouteWaypoints,
    TerminalWaypoints,
    Sids,
    Stars,
    Iaps,
    Airways,
}

type StepRunner = fn(&StepDefinition, u32, &PathsConfig) -> Result<()>;

struct StepDefinition {
    step: Step,
    name: &'static str,
    runner: StepRunner,
}

const STEP_REGISTRY: &[StepDefinition] = &[
    StepDefinition {
        step: Step::Airports,
        name: "airports",
        runner: run_airports_step,
    },
    StepDefinition {
        step: Step::Runways,
        name: "runways",
        runner: run_runways_step,
    },
    StepDefinition {
        step: Step::Vhfs,
        name: "vhfs",
        runner: run_vhfs_step,
    },
    StepDefinition {
        step: Step::Gs,
        name: "gs",
        runner: run_gs_step,
    },
    StepDefinition {
        step: Step::Ndbs,
        name: "ndbs",
        runner: run_ndbs_step,
    },
    StepDefinition {
        step: Step::EnrouteWaypoints,
        name: "enroute_waypoints",
        runner: run_enroute_waypoints_step,
    },
    StepDefinition {
        step: Step::TerminalWaypoints,
        name: "terminal_waypoints",
        runner: run_terminal_waypoints_step,
    },
    StepDefinition {
        step: Step::Sids,
        name: "sids",
        runner: run_sids_step,
    },
    StepDefinition {
        step: Step::Stars,
        name: "stars",
        runner: run_stars_step,
    },
    StepDefinition {
        step: Step::Iaps,
        name: "iaps",
        runner: run_iaps_step,
    },
    StepDefinition {
        step: Step::Airways,
        name: "airways",
        runner: run_airways_step,
    },
];

#[derive(Parser, Debug)]
#[command(name = "pmdg_navdata_cli")]
#[command(about = "Rust CLI for building PMDG navdata databases")]
struct Cli {
    #[arg(long)]
    config: Option<PathBuf>,

    #[arg(long, value_enum, default_value = "all")]
    step: Step,

    #[arg(long, default_value_t = 30)]
    timeout: u32,

    #[arg(long)]
    skip_postprocess: bool,
}

#[derive(Debug)]
struct PathsConfig {
    db_output_path: String,
    master_ndb_path: String,
    cifp_path: String,
    path_to_fix_dat: String,
    path_to_nav_dat: String,
    path_to_rwy_direction_csv: String,
    path_to_rwy_csv: String,
    path_to_ad_hp_csv: String,
    path_to_rte_seg_csv: String,
    path_to_vor_csv: String,
    path_to_ndb_csv: String,
}

fn main() {
    match run() {
        Ok(()) => pause_on_exit(),
        Err(err) => {
            eprintln!("error: {err:#}");
            pause_on_exit();
            std::process::exit(1);
        }
    }
}

#[cfg(windows)]
fn pause_on_exit() {
    use std::io::Write;

    eprintln!();
    eprint!("Press Enter to exit...");
    let _ = std::io::stderr().flush();

    let mut input = String::new();
    let _ = std::io::stdin().read_line(&mut input);
}

#[cfg(not(windows))]
fn pause_on_exit() {}

fn run() -> Result<()> {
    if let Some(log_path) = init_cli_logging()? {
        info!("CLI logging initialized: {}", log_path.display());
    }

    let cli = Cli::parse();
    let config_path = resolve_config_path(cli.config.as_deref())?;
    let config = load_config(&config_path)?;

    println!("Using config: {}", config_path.display());
    info!("Using config: {}", config_path.display());

    run_cli(&cli, &config)
}

fn run_cli(cli: &Cli, config: &PathsConfig) -> Result<()> {
    let started_at = Instant::now();
    run_native_steps(cli.step, cli.timeout, config)?;

    if !cli.skip_postprocess {
        println!("[post] dropping non-system indexes");
        info!("Dropping non-system indexes");
        let _ = drop_non_system_indexes_native(&config.db_output_path, cli.timeout, 5, 2000)?;
        println!("[post] vacuum");
        info!("Vacuuming SQLite database");
        let _ = vacuum_native(&config.db_output_path, cli.timeout, 5, 2000)?;
    }

    println!("Completed in {:.2}s", started_at.elapsed().as_secs_f64());
    info!("Completed in {:.2}s", started_at.elapsed().as_secs_f64());
    Ok(())
}

fn run_native_steps(step: Step, timeout: u32, config: &PathsConfig) -> Result<()> {
    let shared_conn = open_sqlite_connection(&config.db_output_path, timeout)?;
    shared_conn.optimize_native()?;
    set_shared_connection(&config.db_output_path, &shared_conn)?;

    let result = execute_selected_steps(step, timeout, config);
    let close_result = close_shared_connection(&config.db_output_path, true);
    if close_result.is_ok() {
        info!("Closed shared SQLite connection");
    }

    result?;
    close_result?;
    Ok(())
}

fn execute_selected_steps(step: Step, timeout: u32, config: &PathsConfig) -> Result<()> {
    if step == Step::All {
        for definition in STEP_REGISTRY {
            (definition.runner)(definition, timeout, config)?;
        }
        return Ok(());
    }

    execute_registered_step(step, timeout, config)
}

fn execute_registered_step(step: Step, timeout: u32, config: &PathsConfig) -> Result<()> {
    let Some(definition) = STEP_REGISTRY
        .iter()
        .find(|definition| definition.step == step)
    else {
        bail!("step is not registered: {:?}", step);
    };
    (definition.runner)(definition, timeout, config)
}

fn run_count_step(step_name: &str, action: impl FnOnce() -> Result<usize>) -> Result<()> {
    run_step(step_name, || log_insert_count(step_name, action()))?;
    Ok(())
}

fn run_airports_step(
    definition: &StepDefinition,
    _timeout: u32,
    config: &PathsConfig,
) -> Result<()> {
    run_step(definition.name, || {
        let (inserted_count, inserted_zlyx) = process_airports_to_db(
            &config.path_to_ad_hp_csv,
            &config.master_ndb_path,
            &shared_connection(&config.db_output_path)?,
        )?;
        info!(
            "Airports complete: inserted={}, inserted_zlyx={}",
            inserted_count, inserted_zlyx
        );
        Ok(())
    })?;
    Ok(())
}

fn run_runways_step(
    definition: &StepDefinition,
    _timeout: u32,
    config: &PathsConfig,
) -> Result<()> {
    run_step(definition.name, || {
        let (inserted_count, supplementary_count, missing_count, missing_samples) =
            process_runways_to_db(
                &config.master_ndb_path,
                &config.path_to_rwy_direction_csv,
                &config.path_to_rwy_csv,
                &config.path_to_ad_hp_csv,
                &shared_connection(&config.db_output_path)?,
            )?;
        info!(
            "Runways complete: inserted={}, supplementary={}, missing={}",
            inserted_count, supplementary_count, missing_count
        );
        if missing_count > 0 {
            info!(
                "Runway missing coordinate samples: {}",
                missing_samples.join("; ")
            );
        }
        Ok(())
    })?;
    Ok(())
}

fn run_vhfs_step(definition: &StepDefinition, _timeout: u32, config: &PathsConfig) -> Result<()> {
    run_count_step(definition.name, || {
        process_vhfs_to_db(
            &config.path_to_nav_dat,
            &config.path_to_vor_csv,
            &config.path_to_ndb_csv,
            &shared_connection(&config.db_output_path)?,
        )
    })
}

fn run_gs_step(definition: &StepDefinition, _timeout: u32, config: &PathsConfig) -> Result<()> {
    run_count_step(definition.name, || {
        process_ils_gs_to_db(
            &config.path_to_nav_dat,
            &shared_connection(&config.db_output_path)?,
        )
    })
}

fn run_ndbs_step(definition: &StepDefinition, _timeout: u32, config: &PathsConfig) -> Result<()> {
    run_count_step(definition.name, || {
        process_ndbs_to_db(
            &config.path_to_nav_dat,
            &shared_connection(&config.db_output_path)?,
        )
    })
}

fn run_enroute_waypoints_step(
    definition: &StepDefinition,
    _timeout: u32,
    config: &PathsConfig,
) -> Result<()> {
    run_count_step(definition.name, || {
        process_enroute_waypoints_to_db(
            &config.path_to_fix_dat,
            &shared_connection(&config.db_output_path)?,
        )
    })
}

fn run_terminal_waypoints_step(
    definition: &StepDefinition,
    timeout: u32,
    config: &PathsConfig,
) -> Result<()> {
    run_step(definition.name, || {
        let (parsed_count, new_count) = process_terminal_waypoints_file_to_db(
            &config.path_to_fix_dat,
            &config.db_output_path,
            "tbl_terminal_waypoints",
            timeout,
            500,
            1000,
        )?;
        info!(
            "Terminal waypoints complete: parsed={}, inserted={}",
            parsed_count, new_count
        );
        Ok(())
    })?;
    Ok(())
}

fn run_sids_step(definition: &StepDefinition, timeout: u32, config: &PathsConfig) -> Result<()> {
    run_terminal_cifp_step(definition.name, &SIDS_STEP_CONFIG, timeout, config)
}

fn run_stars_step(definition: &StepDefinition, timeout: u32, config: &PathsConfig) -> Result<()> {
    run_terminal_cifp_step(definition.name, &STARS_STEP_CONFIG, timeout, config)
}

fn run_iaps_step(definition: &StepDefinition, timeout: u32, config: &PathsConfig) -> Result<()> {
    run_terminal_cifp_step(definition.name, &IAPS_STEP_CONFIG, timeout, config)
}

fn run_airways_step(
    definition: &StepDefinition,
    _timeout: u32,
    config: &PathsConfig,
) -> Result<()> {
    run_step(definition.name, || {
        let (final_count, updated_routes) = process_airways_to_db(
            &config.path_to_rte_seg_csv,
            &config.path_to_fix_dat,
            &config.path_to_nav_dat,
            &shared_connection(&config.db_output_path)?,
        )?;
        info!(
            "Airways complete: final_rows={}, updated_routes={}",
            final_count, updated_routes
        );
        Ok(())
    })?;
    Ok(())
}

fn run_terminal_cifp_step(
    step_name: &str,
    step_config: &TerminalCifpStepConfig,
    timeout: u32,
    config: &PathsConfig,
) -> Result<()> {
    let accepted_prefixes = step_config
        .prefixes
        .iter()
        .map(|item| (*item).to_string())
        .collect::<Vec<_>>();
    let procedure_config = TerminalProcedureConfig {
        table_name: step_config.table_name.to_string(),
        cifp_prefix: step_config.procedure_prefix.to_string(),
        seqno_start: step_config.airport_col_idx,
        seqno_end: step_config.procedure_col_idx,
        airport_prefixes: accepted_prefixes,
        compute_auth: step_config.is_sid,
        use_iaps_logic: step_config.is_iap,
        batch_size: 2000,
        min_fields: 18,
    };

    run_step(step_name, || {
        log_terminal_proc(
            step_name,
            process_terminal_cifp_to_db(
                &config.cifp_path,
                Some(config.path_to_fix_dat.clone()),
                Some(config.path_to_nav_dat.clone()),
                &config.db_output_path,
                &procedure_config,
                timeout,
            ),
        )
    })?;

    Ok(())
}

fn log_terminal_proc(step_name: &str, result: Result<(usize, usize)>) -> Result<()> {
    let (airport_count, total_processed) = result?;
    info!(
        "{} complete: airports={}, processed={}",
        step_name, airport_count, total_processed
    );
    Ok(())
}

fn log_insert_count(step_name: &str, result: Result<usize>) -> Result<()> {
    let inserted_count = result?;
    info!("{} complete: inserted={}", step_name, inserted_count);
    Ok(())
}

fn run_step<T>(name: &str, action: impl FnOnce() -> Result<T>) -> Result<T> {
    let started_at = Instant::now();
    println!("[step] {name}");
    let result = action()?;
    println!("[done] {name} ({:.2}s)", started_at.elapsed().as_secs_f64());
    Ok(result)
}

fn shared_connection(db_path: &str) -> Result<RustSqliteConnection> {
    get_shared_connection(db_path)?
        .ok_or_else(|| anyhow::anyhow!("shared connection not available for {db_path}"))
}

fn with_native_connection<T>(
    db_path: &str,
    timeout: u32,
    action: impl FnOnce(&RustSqliteConnection) -> Result<T>,
) -> Result<T> {
    let conn = RustSqliteConnection::open_native(db_path, timeout)
        .map_err(|err| anyhow::anyhow!(err.to_string()))?;
    let result = action(&conn);
    conn.close_native();
    result
}

fn drop_non_system_indexes_native(
    db_path: &str,
    timeout: u32,
    retries: usize,
    retry_delay_ms: u64,
) -> Result<bool> {
    for _ in 0..retries {
        let result = with_native_connection(db_path, timeout, |conn| {
            let mut indexes = Vec::new();
            conn.query_each_native(
                "SELECT name FROM sqlite_master WHERE type='index' AND name NOT LIKE 'sqlite_%';",
                &[],
                |row| {
                    indexes.push(row.get::<_, String>(0)?);
                    Ok(())
                },
            )
            .map_err(|err| anyhow::anyhow!(err.to_string()))?;

            for index_name in indexes {
                let sql = format!(
                    "DROP INDEX IF EXISTS {}",
                    quote_sqlite_identifier(&index_name)
                );
                conn.execute_statement_native(&sql, &[])
                    .map_err(|err| anyhow::anyhow!(err.to_string()))?;
            }

            Ok(true)
        });

        match result {
            Ok(value) => return Ok(value),
            Err(err) if is_locked_error_message(&err) => {
                thread::sleep(Duration::from_millis(retry_delay_ms));
                continue;
            }
            Err(err) => return Err(err),
        }
    }

    Ok(false)
}

fn vacuum_native(db_path: &str, timeout: u32, retries: usize, retry_delay_ms: u64) -> Result<bool> {
    for _ in 0..retries {
        let result = with_native_connection(db_path, timeout, |conn| {
            conn.execute_statement_native("VACUUM", &[])
                .map_err(|err| anyhow::anyhow!(err.to_string()))?;
            Ok(true)
        });

        match result {
            Ok(value) => return Ok(value),
            Err(err) if is_locked_error_message(&err) => {
                thread::sleep(Duration::from_millis(retry_delay_ms));
                continue;
            }
            Err(err) => return Err(err),
        }
    }

    Ok(false)
}

fn is_locked_error_message(err: &anyhow::Error) -> bool {
    err.to_string()
        .to_ascii_lowercase()
        .contains("database is locked")
}

fn resolve_config_path(cli_path: Option<&Path>) -> Result<PathBuf> {
    if let Some(path) = cli_path {
        return Ok(path.to_path_buf());
    }

    let exe_dir = std::env::current_exe()
        .context("failed to resolve current executable path")?
        .parent()
        .map(Path::to_path_buf)
        .context("failed to resolve executable directory")?;

    let exe_candidate = exe_dir.join("config.ini");
    if exe_candidate.is_file() {
        return Ok(exe_candidate);
    }

    bail!("config.ini not found next to the executable; create it from config.example.ini or pass --config <path>")
}

fn load_config(config_path: &Path) -> Result<PathsConfig> {
    let mut ini = Ini::new();
    ini.load(config_path.to_string_lossy().as_ref())
        .map_err(|err| anyhow::anyhow!(err))?;

    let xplane_path = ini.get("paths", "XPLANE_PATH").unwrap_or_default();
    let naip_path = required_value(&ini, "NAIP_PATH")?;

    let db_output_path = required_value(&ini, "DB_OUTPUT_PATH")?;
    let master_ndb_path = required_value(&ini, "MASTER_NDB_PATH")?;
    let cifp_path =
        resolve_configured_path(&xplane_path, &required_value(&ini, "CIFP_PATH")?, true);
    let path_to_fix_dat = resolve_configured_path(
        &xplane_path,
        &required_value(&ini, "PATH_TO_FIX_DAT")?,
        true,
    );
    let path_to_nav_dat = resolve_configured_path(
        &xplane_path,
        &required_value(&ini, "PATH_TO_NAV_DAT")?,
        true,
    );

    let path_to_rwy_direction_csv = optional_or_default(
        &ini,
        "PATH_TO_RWY_DIRECTION_CSV",
        Path::new(&naip_path).join("RWY_DIRECTION.csv"),
    );
    let path_to_rwy_csv = optional_or_default(
        &ini,
        "PATH_TO_RWY_CSV",
        Path::new(&naip_path).join("RWY.csv"),
    );
    let path_to_ad_hp_csv = optional_or_default(
        &ini,
        "PATH_TO_AD_HP_CSV",
        Path::new(&naip_path).join("AD_HP.csv"),
    );
    let path_to_rte_seg_csv = optional_or_default(
        &ini,
        "PATH_TO_RTE_SEG_CSV",
        Path::new(&naip_path).join("RTE_SEG.csv"),
    );
    let path_to_vor_csv = optional_or_default(
        &ini,
        "PATH_TO_VOR_CSV",
        Path::new(&naip_path).join("VOR.csv"),
    );
    let path_to_ndb_csv = optional_or_default(
        &ini,
        "PATH_TO_NDB_CSV",
        Path::new(&naip_path).join("NDB.csv"),
    );

    let config = PathsConfig {
        db_output_path,
        master_ndb_path,
        cifp_path,
        path_to_fix_dat,
        path_to_nav_dat,
        path_to_rwy_direction_csv,
        path_to_rwy_csv,
        path_to_ad_hp_csv,
        path_to_rte_seg_csv,
        path_to_vor_csv,
        path_to_ndb_csv,
    };

    validate_config_paths(&config)?;
    Ok(config)
}

fn required_value(ini: &Ini, key: &str) -> Result<String> {
    let value = ini.get("paths", key).unwrap_or_default().trim().to_string();
    if value.is_empty() {
        bail!("missing required [paths] key: {key}");
    }
    Ok(value)
}

fn resolve_configured_path(base: &str, value: &str, allow_relative_to_base: bool) -> String {
    let candidate = PathBuf::from(value);
    if candidate.is_absolute() || !allow_relative_to_base || base.trim().is_empty() {
        return candidate.to_string_lossy().into_owned();
    }
    Path::new(base)
        .join(candidate)
        .to_string_lossy()
        .into_owned()
}

fn optional_or_default(ini: &Ini, key: &str, default: PathBuf) -> String {
    let configured = ini.get("paths", key).unwrap_or_default();
    if configured.trim().is_empty() {
        return default.to_string_lossy().into_owned();
    }
    configured
}

fn validate_config_paths(config: &PathsConfig) -> Result<()> {
    for (name, value) in [
        ("DB_OUTPUT_PATH", config.db_output_path.as_str()),
        ("MASTER_NDB_PATH", config.master_ndb_path.as_str()),
        ("CIFP_PATH", config.cifp_path.as_str()),
        ("PATH_TO_FIX_DAT", config.path_to_fix_dat.as_str()),
        ("PATH_TO_NAV_DAT", config.path_to_nav_dat.as_str()),
        (
            "PATH_TO_RWY_DIRECTION_CSV",
            config.path_to_rwy_direction_csv.as_str(),
        ),
        ("PATH_TO_RWY_CSV", config.path_to_rwy_csv.as_str()),
        ("PATH_TO_AD_HP_CSV", config.path_to_ad_hp_csv.as_str()),
        ("PATH_TO_RTE_SEG_CSV", config.path_to_rte_seg_csv.as_str()),
        ("PATH_TO_VOR_CSV", config.path_to_vor_csv.as_str()),
        ("PATH_TO_NDB_CSV", config.path_to_ndb_csv.as_str()),
    ] {
        if !Path::new(value).exists() {
            bail!("configured path does not exist: {name}={value}");
        }
    }
    Ok(())
}
