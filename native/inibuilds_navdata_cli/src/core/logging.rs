use anyhow::{Context, Result};
use chrono::Local;
use simplelog::{
    ColorChoice, CombinedLogger, Config, LevelFilter, SharedLogger, TermLogger, TerminalMode,
    WriteLogger,
};
use std::fs::{self, File};
use std::path::PathBuf;
use std::sync::Once;

static INIT_LOGGING: Once = Once::new();

pub(crate) fn init_cli_logging() -> Result<Option<PathBuf>> {
    let mut init_result: Result<Option<PathBuf>> = Ok(None);

    INIT_LOGGING.call_once(|| {
        init_result = init_cli_logging_once();
    });

    init_result
}

fn init_cli_logging_once() -> Result<Option<PathBuf>> {
    let exe_dir = std::env::current_exe()
        .context("failed to resolve current executable path")?
        .parent()
        .map(|path| path.to_path_buf())
        .context("failed to resolve executable directory")?;

    let log_dir = exe_dir.join("logs");
    fs::create_dir_all(&log_dir)
        .with_context(|| format!("failed to create log directory: {}", log_dir.display()))?;

    let log_path = log_dir.join(format!(
        "xp2ini_{}.log",
        Local::now().format("%Y%m%d_%H%M%S")
    ));
    let log_file = File::create(&log_path)
        .with_context(|| format!("failed to create log file: {}", log_path.display()))?;

    let mut loggers: Vec<Box<dyn SharedLogger>> = Vec::new();
    let term_logger = TermLogger::new(
        LevelFilter::Info,
        Config::default(),
        TerminalMode::Mixed,
        ColorChoice::Auto,
    );
    loggers.push(term_logger);
    loggers.push(WriteLogger::new(
        LevelFilter::Info,
        Config::default(),
        log_file,
    ));

    CombinedLogger::init(loggers).context("failed to initialize logger")?;
    Ok(Some(log_path))
}
