use anyhow::Result;
use rusqlite::types::Value as SqlValue;
use rusqlite::{params_from_iter, Connection, OpenFlags};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

const SQLITE_OPTIMIZATIONS: &[&str] = &[
    "PRAGMA journal_mode = MEMORY",
    "PRAGMA synchronous = OFF",
    "PRAGMA cache_size = -64000",
    "PRAGMA temp_store = MEMORY",
    "PRAGMA mmap_size = 268435456"
];

const SQLITE_RESTORE_PRAGMAS: &[&str] = &[
    "PRAGMA journal_mode = DELETE",
    "PRAGMA synchronous = FULL",
    "PRAGMA cache_size = -2000",
    "PRAGMA temp_store = DEFAULT",
    "PRAGMA mmap_size = 0"
];

const NAV_ID_INDEX_STATEMENTS: &[&str] = &[
    "CREATE INDEX IF NOT EXISTS idx_tbl_enroute_ndbnavaids_id ON tbl_enroute_ndbnavaids (ndb_identifier)",
    "CREATE INDEX IF NOT EXISTS idx_tbl_vhfnavaids_id ON tbl_vhfnavaids (vor_identifier)",
    "CREATE INDEX IF NOT EXISTS idx_tbl_terminal_ndbnavaids_id ON tbl_terminal_ndbnavaids (ndb_identifier)",
    "CREATE INDEX IF NOT EXISTS idx_tbl_enroute_waypoints_id ON tbl_enroute_waypoints (waypoint_identifier)",
    "CREATE INDEX IF NOT EXISTS idx_tbl_terminal_waypoints_id ON tbl_terminal_waypoints (waypoint_identifier)",
    "CREATE INDEX IF NOT EXISTS idx_tbl_airports_id ON tbl_airports (airport_identifier)",
];

type ConnectionHandle = Arc<Mutex<Option<Connection>>>;

static SHARED_CONNECTIONS: OnceLock<Mutex<HashMap<String, ConnectionHandle>>> = OnceLock::new();
static CREATED_INDEX_GROUPS: OnceLock<Mutex<HashSet<(String, String)>>> = OnceLock::new();

#[derive(Clone)]
pub(crate) struct RustSqliteConnection {
    conn: ConnectionHandle,
}

impl RustSqliteConnection {
    pub fn open_native(db_path: &str, timeout: u32) -> rusqlite::Result<Self> {
        let conn = Connection::open_with_flags(
            db_path,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        )?;
        conn.busy_timeout(Duration::from_secs(timeout as u64))?;
        Ok(Self {
            conn: Arc::new(Mutex::new(Some(conn))),
        })
    }

    pub fn open_read_only_native(db_path: &str, timeout: u32) -> rusqlite::Result<Self> {
        let conn = Connection::open_with_flags(db_path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        conn.busy_timeout(Duration::from_secs(timeout as u64))?;
        Ok(Self {
            conn: Arc::new(Mutex::new(Some(conn))),
        })
    }

    pub fn query_each_native<F>(
        &self,
        sql: &str,
        params: &[SqlValue],
        mut on_row: F,
    ) -> rusqlite::Result<()>
    where
        F: FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<()>,
    {
        let mut guard = self.conn.lock().unwrap();
        let conn = guard.as_mut().ok_or(rusqlite::Error::InvalidQuery)?;

        let mut stmt = conn.prepare(sql)?;
        if stmt.column_count() == 0 {
            stmt.execute(params_from_iter(params.iter()))?;
            return Ok(());
        }

        let mut rows = stmt.query(params_from_iter(params.iter()))?;
        while let Some(row) = rows.next()? {
            on_row(row)?;
        }
        Ok(())
    }

    pub fn execute_statement_native(&self, sql: &str, params: &[SqlValue]) -> rusqlite::Result<()> {
        self.query_each_native(sql, params, |_| Ok(()))
    }

    pub fn with_connection_native<T>(
        &self,
        action: impl FnOnce(&mut Connection) -> rusqlite::Result<T>,
    ) -> rusqlite::Result<T> {
        let mut guard = self.conn.lock().unwrap();
        let conn = guard.as_mut().ok_or(rusqlite::Error::InvalidQuery)?;
        action(conn)
    }

    pub fn optimize_native(&self) -> rusqlite::Result<()> {
        for pragma in SQLITE_OPTIMIZATIONS {
            self.execute_statement_native(pragma, &[])?;
        }
        Ok(())
    }

    pub fn restore_defaults_native(&self) -> rusqlite::Result<()> {
        self.execute_statement_native("PRAGMA optimize", &[])?;
        self.execute_statement_native("PRAGMA wal_checkpoint(TRUNCATE)", &[])?;
        for pragma in SQLITE_RESTORE_PRAGMAS {
            self.execute_statement_native(pragma, &[])?;
        }
        Ok(())
    }

    pub fn close_native(&self) {
        let mut guard = self.conn.lock().unwrap();
        *guard = None;
    }

    fn from_handle(conn: ConnectionHandle) -> Self {
        Self { conn }
    }

    fn clone_handle(&self) -> ConnectionHandle {
        self.conn.clone()
    }
}

pub(crate) fn quote_sqlite_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn shared_connections() -> &'static Mutex<HashMap<String, ConnectionHandle>> {
    SHARED_CONNECTIONS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn created_index_groups() -> &'static Mutex<HashSet<(String, String)>> {
    CREATED_INDEX_GROUPS.get_or_init(|| Mutex::new(HashSet::new()))
}

fn absolute_path_std(path: &str) -> String {
    let path_ref = Path::new(path);
    if path_ref.is_absolute() {
        return path_ref.to_string_lossy().into_owned();
    }
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    cwd.join(path_ref).to_string_lossy().into_owned()
}

fn normalize_db_path(path: &str) -> String {
    absolute_path_std(path)
}

fn execute_sql_batch(conn: &RustSqliteConnection, statements: &[&str]) -> Result<()> {
    for statement in statements {
        conn.execute_statement_native(statement, &[])?;
    }
    Ok(())
}

fn run_index_group_once_inner<F>(db_path: &str, group_name: &str, create_fn: F) -> Result<bool>
where
    F: FnOnce() -> Result<()>,
{
    let key = (normalize_db_path(db_path), group_name.to_string());
    {
        let created = created_index_groups().lock().unwrap();
        if created.contains(&key) {
            return Ok(false);
        }
    }

    create_fn()?;
    created_index_groups().lock().unwrap().insert(key);
    Ok(true)
}

pub(crate) fn open_sqlite_connection(db_path: &str, timeout: u32) -> Result<RustSqliteConnection> {
    RustSqliteConnection::open_native(db_path, timeout).map_err(Into::into)
}

pub(crate) fn open_sqlite_readonly_connection(
    db_path: &str,
    timeout: u32,
) -> Result<RustSqliteConnection> {
    RustSqliteConnection::open_read_only_native(db_path, timeout).map_err(Into::into)
}

pub(crate) fn set_shared_connection(db_path: &str, conn: &RustSqliteConnection) -> Result<()> {
    let normalized = normalize_db_path(db_path);
    shared_connections()
        .lock()
        .unwrap()
        .insert(normalized, conn.clone_handle());
    Ok(())
}

pub(crate) fn get_shared_connection(db_path: &str) -> Result<Option<RustSqliteConnection>> {
    let normalized = normalize_db_path(db_path);
    Ok(shared_connections()
        .lock()
        .unwrap()
        .get(&normalized)
        .cloned()
        .map(RustSqliteConnection::from_handle))
}

pub(crate) fn restore_database_pragmas_sqlite(db_path: &str, timeout: u32) -> Result<()> {
    let conn = RustSqliteConnection::open_native(db_path, timeout)?;
    let result = (|| {
        conn.execute_statement_native("PRAGMA wal_checkpoint(TRUNCATE)", &[])?;
        for pragma in SQLITE_RESTORE_PRAGMAS {
            conn.execute_statement_native(pragma, &[])?;
        }
        Ok(())
    })();
    conn.close_native();
    result
}

pub(crate) fn close_shared_connection(db_path: &str, restore_on_close: bool) -> Result<()> {
    let normalized = normalize_db_path(db_path);
    let conn = shared_connections().lock().unwrap().remove(&normalized);
    let Some(handle) = conn else {
        return Ok(());
    };

    let conn = RustSqliteConnection::from_handle(handle);
    if restore_on_close {
        conn.restore_defaults_native()?;
    }
    conn.close_native();
    if restore_on_close {
        let _ = restore_database_pragmas_sqlite(db_path, 30);
    }
    Ok(())
}

fn ensure_nav_id_indexes_sqlite(db_path: &str, timeout: u32) -> Result<()> {
    let conn = RustSqliteConnection::open_native(db_path, timeout)?;
    let result = execute_sql_batch(&conn, NAV_ID_INDEX_STATEMENTS);
    conn.close_native();
    result
}

pub(crate) fn ensure_nav_id_indexes(db_path: &str, timeout: u32) -> Result<()> {
    let _ = run_index_group_once_inner(db_path, "nav_id_indexes", || {
        ensure_nav_id_indexes_sqlite(db_path, timeout)
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::quote_sqlite_identifier;

    #[test]
    fn quotes_sqlite_identifier_and_escapes_inner_quotes() {
        assert_eq!(quote_sqlite_identifier("simple_name"), "\"simple_name\"");
        assert_eq!(quote_sqlite_identifier("bad\"name"), "\"bad\"\"name\"");
    }
}
