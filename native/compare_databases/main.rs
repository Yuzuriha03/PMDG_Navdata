use anyhow::{Context, Result};
use clap::Parser;
use rusqlite::types::ValueRef;
use rusqlite::{Connection, Row};
use std::collections::{HashMap, HashSet};
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

const DEFAULT_DB_A: &str = concat!(
    r"C:\Users\Yin_Yizhao\AppData\Local\Packages\Microsoft.Limitless_8wekyb3d8bbwe",
    r"\LocalState\WASM\MSFS2024\pmdg-aircraft-77er\work\NavigationData\e_dfd_PMDG.s3db"
);
const DEFAULT_DB_B: &str = r"E:\yyz\Downloads\e_dfd_PMDG.s3db";
const MAX_SCHEMA_DIFF_LINES: usize = 200;

#[derive(Parser, Debug)]
#[command(name = "compare_databases")]
#[command(about = "比较两个 SQLite 数据库的所有表并输出差异。")]
struct Cli {
    #[arg(long, default_value = DEFAULT_DB_A)]
    db_a: PathBuf,

    #[arg(long, default_value = DEFAULT_DB_B)]
    db_b: PathBuf,

    #[arg(long, default_value_t = 10)]
    sample_limit: usize,

    #[arg(long)]
    skip_data: bool,

    #[arg(long)]
    output: Option<PathBuf>,

    #[arg(long)]
    fail_on_diff: bool,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum CellValue {
    Null,
    Integer(i64),
    Real(u64),
    Text(String),
    Blob(Vec<u8>),
}

impl CellValue {
    fn from_row(row: &Row<'_>, index: usize) -> rusqlite::Result<Self> {
        match row.get_ref(index)? {
            ValueRef::Null => Ok(Self::Null),
            ValueRef::Integer(value) => Ok(Self::Integer(value)),
            ValueRef::Real(value) => Ok(Self::Real(value.to_bits())),
            ValueRef::Text(value) => Ok(Self::Text(String::from_utf8_lossy(value).into_owned())),
            ValueRef::Blob(value) => Ok(Self::Blob(value.to_vec())),
        }
    }

    fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Null => None,
            Self::Integer(value) => Some(*value as f64),
            Self::Real(bits) => Some(f64::from_bits(*bits)),
            Self::Text(value) => {
                let trimmed = value.trim();
                if trimmed.is_empty() {
                    return None;
                }
                trimmed.parse::<f64>().ok()
            }
            Self::Blob(_) => None,
        }
    }

    fn render(&self) -> String {
        match self {
            Self::Null => "null".to_string(),
            Self::Integer(value) => value.to_string(),
            Self::Real(bits) => f64::from_bits(*bits).to_string(),
            Self::Text(value) => format!("{value:?}"),
            Self::Blob(bytes) => {
                let mut rendered = String::from("X'");
                for byte in bytes {
                    let _ = write!(rendered, "{byte:02X}");
                }
                rendered.push('\'');
                rendered
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ColumnInfo {
    name: String,
    data_type: String,
    not_null: bool,
    default_value: Option<String>,
    primary_key_index: i64,
}

#[derive(Clone, Debug)]
struct RowRecord {
    values: Vec<CellValue>,
}

impl RowRecord {
    fn base_key(&self, tolerant_indices: &HashSet<usize>) -> Vec<CellValue> {
        self.values
            .iter()
            .enumerate()
            .filter(|(index, _)| !tolerant_indices.contains(index))
            .map(|(_, value)| value.clone())
            .collect()
    }

    fn to_sample_row(&self, columns: &[String]) -> SampleRow {
        SampleRow::from_columns_and_values(columns, &self.values)
    }
}

#[derive(Clone, Debug)]
struct SampleRow {
    entries: Vec<(String, CellValue)>,
}

impl SampleRow {
    fn from_columns_and_values(columns: &[String], values: &[CellValue]) -> Self {
        let entries = columns
            .iter()
            .cloned()
            .zip(values.iter().cloned())
            .collect::<Vec<_>>();
        Self { entries }
    }

    fn render(&self) -> String {
        let mut text = String::from("{");
        for (index, (name, value)) in self.entries.iter().enumerate() {
            if index > 0 {
                text.push_str(", ");
            }
            let _ = write!(text, "{name:?}: {}", value.render());
        }
        text.push('}');
        text
    }
}

#[derive(Debug)]
struct TableDiff {
    table: String,
    schema_changed: bool,
    row_count_a: Option<i64>,
    row_count_b: Option<i64>,
    groups_only_in_a: Option<usize>,
    groups_only_in_b: Option<usize>,
    schema_detail: Vec<String>,
    sample_only_in_a: Vec<SampleRow>,
    sample_only_in_b: Vec<SampleRow>,
}

impl TableDiff {
    fn has_difference(&self) -> bool {
        self.schema_changed
            || self.row_count_a != self.row_count_b
            || self.groups_only_in_a.unwrap_or(0) > 0
            || self.groups_only_in_b.unwrap_or(0) > 0
    }
}

#[derive(Clone, Copy)]
enum DiffOp {
    Equal,
    Delete,
    Insert,
}

fn main() {
    let exit_code = match run() {
        Ok(code) => code,
        Err(err) => {
            eprintln!("错误: {err:#}");
            2
        }
    };
    std::process::exit(exit_code);
}

fn run() -> Result<i32> {
    let cli = Cli::parse();

    if !cli.db_a.exists() {
        eprintln!("错误: 数据库 A 不存在: {}", cli.db_a.display());
        return Ok(2);
    }
    if !cli.db_b.exists() {
        eprintln!("错误: 数据库 B 不存在: {}", cli.db_b.display());
        return Ok(2);
    }

    let conn = Connection::open(&cli.db_a)
        .with_context(|| format!("failed to open {}", cli.db_a.display()))?;
    conn.execute(
        "ATTACH DATABASE ? AS db_b",
        [cli.db_b.to_string_lossy().as_ref()],
    )
    .with_context(|| format!("failed to attach {}", cli.db_b.display()))?;

    let tables_a = fetch_table_names(&conn, "main")?;
    let tables_b = fetch_table_names(&conn, "db_b")?;

    let set_a = tables_a.iter().cloned().collect::<HashSet<_>>();
    let set_b = tables_b.iter().cloned().collect::<HashSet<_>>();
    let mut only_in_a = set_a.difference(&set_b).cloned().collect::<Vec<_>>();
    let mut only_in_b = set_b.difference(&set_a).cloned().collect::<Vec<_>>();
    let mut common = set_a.intersection(&set_b).cloned().collect::<Vec<_>>();
    only_in_a.sort();
    only_in_b.sort();
    common.sort();

    let mut table_diffs = Vec::with_capacity(common.len());
    for (index, table) in common.iter().enumerate() {
        println!("[{}/{}] 比较表: {}", index + 1, common.len(), table);
        table_diffs.push(compare_table(
            &conn,
            table,
            cli.sample_limit,
            cli.skip_data,
        )?);
    }

    let report = build_report(&cli.db_a, &cli.db_b, &only_in_a, &only_in_b, &table_diffs);
    println!();
    println!("{report}");

    if let Some(output_path) = cli.output.as_ref() {
        let mut content = String::from("\u{feff}");
        content.push_str(&report);
        std::fs::write(output_path, content)
            .with_context(|| format!("failed to write {}", output_path.display()))?;
        println!();
        println!("报告已写入: {}", output_path.display());
    }

    let diff_exists = !only_in_a.is_empty()
        || !only_in_b.is_empty()
        || table_diffs.iter().any(TableDiff::has_difference);
    Ok(exit_code_for_diff(diff_exists, cli.fail_on_diff))
}

fn exit_code_for_diff(diff_exists: bool, fail_on_diff: bool) -> i32 {
    if diff_exists && fail_on_diff {
        1
    } else {
        0
    }
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn fq_table(schema: &str, table: &str) -> String {
    format!("{schema}.{}", quote_ident(table))
}

fn fetch_table_names(conn: &Connection, schema: &str) -> Result<Vec<String>> {
    let sql = format!(
        "SELECT name FROM {schema}.sqlite_master \
         WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
    let mut tables = Vec::new();
    for row in rows {
        tables.push(row?);
    }
    Ok(tables)
}

fn fetch_create_sql(conn: &Connection, schema: &str, table: &str) -> Result<String> {
    let sql = format!("SELECT sql FROM {schema}.sqlite_master WHERE type='table' AND name=?1");
    let row = conn.query_row(&sql, [table], |row| row.get::<_, Option<String>>(0));
    Ok(row?.unwrap_or_default().trim().to_string())
}

fn fetch_columns(conn: &Connection, schema: &str, table: &str) -> Result<Vec<ColumnInfo>> {
    let sql = format!("PRAGMA {schema}.table_info({})", quote_ident(table));
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map([], |row| {
        Ok(ColumnInfo {
            name: row.get(1)?,
            data_type: row.get::<_, Option<String>>(2)?.unwrap_or_default(),
            not_null: row.get::<_, i64>(3)? != 0,
            default_value: row.get(4)?,
            primary_key_index: row.get(5)?,
        })
    })?;

    let mut columns = Vec::new();
    for row in rows {
        columns.push(row?);
    }
    Ok(columns)
}

fn columns_signature(columns: &[ColumnInfo]) -> Vec<(String, String, bool, Option<String>, i64)> {
    columns
        .iter()
        .map(|column| {
            (
                column.name.clone(),
                column.data_type.clone(),
                column.not_null,
                column.default_value.clone(),
                column.primary_key_index,
            )
        })
        .collect()
}

fn build_schema_diff_lines(create_a: &str, create_b: &str) -> Vec<String> {
    let lines_a = if create_a.is_empty() {
        vec!["<EMPTY>".to_string()]
    } else {
        create_a.lines().map(str::to_string).collect::<Vec<_>>()
    };
    let lines_b = if create_b.is_empty() {
        vec!["<EMPTY>".to_string()]
    } else {
        create_b.lines().map(str::to_string).collect::<Vec<_>>()
    };

    if lines_a == lines_b {
        return Vec::new();
    }

    let operations = diff_operations(&lines_a, &lines_b);
    let mut diff_lines = Vec::with_capacity(operations.len() + 3);
    diff_lines.push("--- db_a".to_string());
    diff_lines.push("+++ db_b".to_string());
    diff_lines.push(format!("@@ -1,{} +1,{} @@", lines_a.len(), lines_b.len()));

    for (op, line) in operations {
        let prefix = match op {
            DiffOp::Equal => ' ',
            DiffOp::Delete => '-',
            DiffOp::Insert => '+',
        };
        diff_lines.push(format!("{prefix}{line}"));
    }

    diff_lines
}

fn diff_operations(left: &[String], right: &[String]) -> Vec<(DiffOp, String)> {
    let mut lcs = vec![vec![0usize; right.len() + 1]; left.len() + 1];
    for left_index in (0..left.len()).rev() {
        for right_index in (0..right.len()).rev() {
            lcs[left_index][right_index] = if left[left_index] == right[right_index] {
                lcs[left_index + 1][right_index + 1] + 1
            } else {
                lcs[left_index + 1][right_index].max(lcs[left_index][right_index + 1])
            };
        }
    }

    let mut result = Vec::new();
    let (mut left_index, mut right_index) = (0usize, 0usize);
    while left_index < left.len() && right_index < right.len() {
        if left[left_index] == right[right_index] {
            result.push((DiffOp::Equal, left[left_index].clone()));
            left_index += 1;
            right_index += 1;
        } else if lcs[left_index + 1][right_index] >= lcs[left_index][right_index + 1] {
            result.push((DiffOp::Delete, left[left_index].clone()));
            left_index += 1;
        } else {
            result.push((DiffOp::Insert, right[right_index].clone()));
            right_index += 1;
        }
    }
    while left_index < left.len() {
        result.push((DiffOp::Delete, left[left_index].clone()));
        left_index += 1;
    }
    while right_index < right.len() {
        result.push((DiffOp::Insert, right[right_index].clone()));
        right_index += 1;
    }
    result
}

fn count_rows(conn: &Connection, schema: &str, table: &str) -> Result<i64> {
    let sql = format!("SELECT COUNT(*) FROM {}", fq_table(schema, table));
    let count = conn.query_row(&sql, [], |row| row.get::<_, i64>(0))?;
    Ok(count)
}

fn configured_normalization_digits(table: &str, column_name: &str) -> Option<u32> {
    if table_name_matches(table, "tbl_airports") {
        return match column_name {
            "airport_ref_latitude" | "airport_ref_longitude" => Some(8),
            _ => None,
        };
    }

    if table_name_matches(table, "tbl_enroute_ndbnavaids") {
        return match column_name {
            "ndb_latitude" | "ndb_longitude" | "navaid_latitude" | "navaid_longitude" => {
                Some(8)
            }
            _ => None,
        };
    }

    if table_name_matches(table, "tbl_vhfnavaids") {
        return match column_name {
            "dme_latitude" | "dme_longitude" | "vor_latitude" | "vor_longitude"
            | "navaid_latitude" | "navaid_longitude" => Some(8),
            _ => None,
        };
    }

    if table_name_matches(table, "tbl_enroute_waypoints")
        || table_name_matches(table, "tbl_enroute_airways")
        || table_name_matches(table, "tbl_terminal_waypoints")
    {
        return match column_name {
            "waypoint_latitude" | "waypoint_longitude" => Some(8),
            _ => None,
        };
    }

    if table_name_matches(table, "tbl_sids")
        || table_name_matches(table, "tbl_stars")
        || table_name_matches(table, "tbl_iaps")
    {
        return match column_name {
            "center_waypoint_latitude"
            | "center_waypoint_longitude"
            | "recommended_navaid_latitude"
            | "recommended_navaid_longitude"
            | "recommanded_navaid_latitude"
            | "recommanded_navaid_longitude"
            | "waypoint_latitude"
            | "waypoint_longitude" => Some(8),
            _ => None,
        };
    }

    if table_name_matches(table, "tbl_localizers_glideslopes") {
        return match column_name {
            "gs_latitude" | "gs_longitude" | "llz_latitude" | "llz_longitude" => Some(8),
            _ => None,
        };
    }

    None
}

fn configured_tolerance(table: &str, column_name: &str) -> Option<f64> {
    if table_name_matches(table, "tbl_enroute_ndbnavaids") {
        return match column_name {
            "magnetic_variation" => Some(0.5),
            _ => None,
        };
    }

    if table_name_matches(table, "tbl_vhfnavaids") {
        return match column_name {
            "magnetic_variation" | "station_declination" => Some(0.5),
            _ => None,
        };
    }

    if table_name_matches(table, "tbl_enroute_waypoints")
        || table_name_matches(table, "tbl_terminal_waypoints")
    {
        return match column_name {
            "magnetic_variation" => Some(0.5),
            _ => None,
        };
    }

    if table_name_matches(table, "tbl_enroute_airways") {
        return match column_name {
            "inbound_course" | "outbound_course" => Some(1.0),
            _ => None,
        };
    }

    if table_name_matches(table, "tbl_localizers_glideslopes") {
        return match column_name {
            "llz_bearing" => Some(1.0),
            "station_declination" => Some(0.5),
            _ => None,
        };
    }

    None
}

fn table_name_matches(table_name: &str, canonical_name: &str) -> bool {
    if table_name == canonical_name {
        return true;
    }

    let Some(name_body) = table_name.strip_prefix("tbl_") else {
        return false;
    };
    let Some(canonical_body) = canonical_name.strip_prefix("tbl_") else {
        return false;
    };
    let Some((legacy_prefix, remainder)) = name_body.split_once('_') else {
        return false;
    };

    legacy_prefix.len() <= 2 && remainder == canonical_body
}

fn comparison_normalizations(table: &str, columns: &[ColumnInfo]) -> HashMap<usize, u32> {
    columns
        .iter()
        .enumerate()
        .filter_map(|(index, column)| {
            configured_normalization_digits(table, &column.name).map(|digits| (index, digits))
        })
        .collect()
}

fn comparison_tolerances(table: &str, columns: &[ColumnInfo]) -> HashMap<usize, f64> {
    columns
        .iter()
        .enumerate()
        .filter_map(|(index, column)| {
            configured_tolerance(table, &column.name).map(|tolerance| (index, tolerance))
        })
        .collect()
}

fn normalized_group_expr(column_name: &str, digits: Option<u32>) -> String {
    let quoted = quote_ident(column_name);
    match digits {
        Some(digits) => format!("ROUND(CAST({quoted} AS REAL), {digits})"),
        None => quoted,
    }
}

fn normalized_select_expr(column_name: &str, digits: Option<u32>) -> String {
    match digits {
        Some(digits) => format!(
            "{} AS {}",
            normalized_group_expr(column_name, Some(digits)),
            quote_ident(column_name)
        ),
        None => quote_ident(column_name),
    }
}

fn column_exprs(table: &str, columns: &[ColumnInfo]) -> (String, String) {
    let normalizations = comparison_normalizations(table, columns);
    let mut select_exprs = Vec::with_capacity(columns.len());
    let mut group_exprs = Vec::with_capacity(columns.len());
    for (index, column) in columns.iter().enumerate() {
        let digits = normalizations.get(&index).copied();
        select_exprs.push(normalized_select_expr(&column.name, digits));
        group_exprs.push(normalized_group_expr(&column.name, digits));
    }
    (select_exprs.join(", "), group_exprs.join(", "))
}

fn fetch_rows_as_records(
    conn: &Connection,
    schema: &str,
    table: &str,
    columns: &[ColumnInfo],
) -> Result<Vec<RowRecord>> {
    let (select_exprs, _) = column_exprs(table, columns);
    let sql = format!("SELECT {select_exprs} FROM {}", fq_table(schema, table));
    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query([])?;
    let mut result = Vec::new();
    while let Some(row) = rows.next()? {
        let mut values = Vec::with_capacity(columns.len());
        for index in 0..columns.len() {
            values.push(CellValue::from_row(row, index)?);
        }
        result.push(RowRecord { values });
    }
    Ok(result)
}

fn within_tolerance(left: &CellValue, right: &CellValue, tolerance: f64) -> bool {
    if left == right {
        return true;
    }

    let Some(left_num) = left.as_f64() else {
        return false;
    };
    let Some(right_num) = right.as_f64() else {
        return false;
    };
    (left_num - right_num).abs() < tolerance + 1e-12
}

fn row_match_distance(
    left: &RowRecord,
    right: &RowRecord,
    tolerant_columns: &HashMap<usize, f64>,
) -> Option<f64> {
    let mut total = 0.0;
    for (index, tolerance) in tolerant_columns {
        let left_value = left.values.get(*index)?;
        let right_value = right.values.get(*index)?;
        if !within_tolerance(left_value, right_value, *tolerance) {
            return None;
        }

        if *tolerance > 0.0 {
            if let (Some(left_num), Some(right_num)) = (left_value.as_f64(), right_value.as_f64()) {
                total += (left_num - right_num).abs() / tolerance;
            }
        }
    }
    Some(total)
}

fn compare_with_tolerance(
    conn: &Connection,
    table: &str,
    columns: &[ColumnInfo],
    tolerant_columns: &HashMap<usize, f64>,
    sample_limit: usize,
) -> Result<(usize, usize, Vec<SampleRow>, Vec<SampleRow>)> {
    let column_names = columns
        .iter()
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    let rows_a = fetch_rows_as_records(conn, "main", table, columns)?;
    let rows_b = fetch_rows_as_records(conn, "db_b", table, columns)?;

    let tolerant_indices = tolerant_columns.keys().copied().collect::<HashSet<_>>();
    let mut groups_a: HashMap<Vec<CellValue>, Vec<RowRecord>> = HashMap::new();
    let mut groups_b: HashMap<Vec<CellValue>, Vec<RowRecord>> = HashMap::new();

    for row in rows_a {
        groups_a
            .entry(row.base_key(&tolerant_indices))
            .or_default()
            .push(row);
    }
    for row in rows_b {
        groups_b
            .entry(row.base_key(&tolerant_indices))
            .or_default()
            .push(row);
    }

    let all_keys = groups_a
        .keys()
        .chain(groups_b.keys())
        .cloned()
        .collect::<HashSet<_>>();
    let mut only_in_a = 0usize;
    let mut only_in_b = 0usize;
    let mut sample_only_in_a = Vec::new();
    let mut sample_only_in_b = Vec::new();

    for key in all_keys {
        let left_rows = groups_a.get(&key).map(Vec::as_slice).unwrap_or(&[]);
        let right_rows = groups_b.get(&key).map(Vec::as_slice).unwrap_or(&[]);
        let mut used_right = HashSet::new();

        for left_row in left_rows {
            let mut best_index = None;
            let mut best_distance = None;

            for (index, right_row) in right_rows.iter().enumerate() {
                if used_right.contains(&index) {
                    continue;
                }
                let Some(distance) = row_match_distance(left_row, right_row, tolerant_columns)
                else {
                    continue;
                };

                if best_distance.is_none_or(|current| distance < current) {
                    best_distance = Some(distance);
                    best_index = Some(index);
                }
            }

            if let Some(index) = best_index {
                used_right.insert(index);
            } else {
                only_in_a += 1;
                if sample_only_in_a.len() < sample_limit {
                    sample_only_in_a.push(left_row.to_sample_row(&column_names));
                }
            }
        }

        for (index, right_row) in right_rows.iter().enumerate() {
            if used_right.contains(&index) {
                continue;
            }
            only_in_b += 1;
            if sample_only_in_b.len() < sample_limit {
                sample_only_in_b.push(right_row.to_sample_row(&column_names));
            }
        }
    }

    Ok((only_in_a, only_in_b, sample_only_in_a, sample_only_in_b))
}

fn count_grouped_diff(
    conn: &Connection,
    left_schema: &str,
    right_schema: &str,
    table: &str,
    columns: &[ColumnInfo],
) -> Result<usize> {
    let (select_exprs, group_exprs) = column_exprs(table, columns);
    let left_table = fq_table(left_schema, table);
    let right_table = fq_table(right_schema, table);
    let sql = format!(
        "
        SELECT COUNT(*) FROM (
            SELECT {select_exprs}, COUNT(*) AS \"__row_count__\"
            FROM {left_table}
            GROUP BY {group_exprs}
            EXCEPT
            SELECT {select_exprs}, COUNT(*) AS \"__row_count__\"
            FROM {right_table}
            GROUP BY {group_exprs}
        )"
    );
    let count = conn.query_row(&sql, [], |row| row.get::<_, i64>(0))?;
    Ok(count.max(0) as usize)
}

fn sample_grouped_diff(
    conn: &Connection,
    left_schema: &str,
    right_schema: &str,
    table: &str,
    columns: &[ColumnInfo],
    limit: usize,
) -> Result<Vec<SampleRow>> {
    let (select_exprs, group_exprs) = column_exprs(table, columns);
    let left_table = fq_table(left_schema, table);
    let right_table = fq_table(right_schema, table);
    let mut column_names = columns
        .iter()
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    column_names.push("__row_count__".to_string());

    let sql = format!(
        "
        SELECT * FROM (
            SELECT {select_exprs}, COUNT(*) AS \"__row_count__\"
            FROM {left_table}
            GROUP BY {group_exprs}
            EXCEPT
            SELECT {select_exprs}, COUNT(*) AS \"__row_count__\"
            FROM {right_table}
            GROUP BY {group_exprs}
        )
        LIMIT ?1"
    );

    let limit = i64::try_from(limit).context("sample limit exceeds i64")?;
    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query([limit])?;
    let mut result = Vec::new();
    while let Some(row) = rows.next()? {
        let mut values = Vec::with_capacity(column_names.len());
        for index in 0..column_names.len() {
            values.push(CellValue::from_row(row, index)?);
        }
        result.push(SampleRow::from_columns_and_values(&column_names, &values));
    }
    Ok(result)
}

fn compare_table(
    conn: &Connection,
    table: &str,
    sample_limit: usize,
    skip_data: bool,
) -> Result<TableDiff> {
    let create_a = fetch_create_sql(conn, "main", table)?;
    let create_b = fetch_create_sql(conn, "db_b", table)?;
    let cols_a = fetch_columns(conn, "main", table)?;
    let cols_b = fetch_columns(conn, "db_b", table)?;

    let sig_a = columns_signature(&cols_a);
    let sig_b = columns_signature(&cols_b);
    let schema_changed = sig_a != sig_b || create_a != create_b;
    let schema_detail = if schema_changed {
        build_schema_diff_lines(&create_a, &create_b)
    } else {
        Vec::new()
    };

    let row_count_a = Some(count_rows(conn, "main", table)?);
    let row_count_b = Some(count_rows(conn, "db_b", table)?);
    let mut groups_only_in_a = None;
    let mut groups_only_in_b = None;
    let mut sample_only_in_a = Vec::new();
    let mut sample_only_in_b = Vec::new();

    let tolerant_columns = comparison_tolerances(table, &cols_a);
    let can_compare_data = !skip_data && sig_a == sig_b && !cols_a.is_empty();
    if can_compare_data {
        if tolerant_columns.is_empty() {
            groups_only_in_a = Some(count_grouped_diff(conn, "main", "db_b", table, &cols_a)?);
            groups_only_in_b = Some(count_grouped_diff(conn, "db_b", "main", table, &cols_a)?);
            if groups_only_in_a.unwrap_or(0) > 0 {
                sample_only_in_a =
                    sample_grouped_diff(conn, "main", "db_b", table, &cols_a, sample_limit)?;
            }
            if groups_only_in_b.unwrap_or(0) > 0 {
                sample_only_in_b =
                    sample_grouped_diff(conn, "db_b", "main", table, &cols_a, sample_limit)?;
            }
        } else {
            let (only_in_a, only_in_b, sample_a, sample_b) =
                compare_with_tolerance(conn, table, &cols_a, &tolerant_columns, sample_limit)?;
            groups_only_in_a = Some(only_in_a);
            groups_only_in_b = Some(only_in_b);
            sample_only_in_a = sample_a;
            sample_only_in_b = sample_b;
        }
    }

    Ok(TableDiff {
        table: table.to_string(),
        schema_changed,
        row_count_a,
        row_count_b,
        groups_only_in_a,
        groups_only_in_b,
        schema_detail,
        sample_only_in_a,
        sample_only_in_b,
    })
}

fn format_sample_rows(rows: &[SampleRow]) -> String {
    if rows.is_empty() {
        return "(无样例)".to_string();
    }

    rows.iter()
        .enumerate()
        .map(|(index, row)| format!("    [{}] {}", index + 1, row.render()))
        .collect::<Vec<_>>()
        .join("\n")
}

fn build_report(
    db_a: &Path,
    db_b: &Path,
    only_in_a: &[String],
    only_in_b: &[String],
    table_diffs: &[TableDiff],
) -> String {
    let mut lines = Vec::new();
    lines.push("=== SQLite 数据库对比报告 ===".to_string());
    lines.push(format!("DB A: {}", db_a.display()));
    lines.push(format!("DB B: {}", db_b.display()));
    lines.push(
        "Note: configured coordinate columns are normalized with ROUND(..., 8) before diffing."
            .to_string(),
    );
    lines.push(String::new());

    lines.push(format!("仅在 DB A 存在的表 ({}):", only_in_a.len()));
    if only_in_a.is_empty() {
        lines.push("  (无)".to_string());
    } else {
        lines.extend(only_in_a.iter().map(|table| format!("  - {table}")));
    }
    lines.push(String::new());

    lines.push(format!("仅在 DB B 存在的表 ({}):", only_in_b.len()));
    if only_in_b.is_empty() {
        lines.push("  (无)".to_string());
    } else {
        lines.extend(only_in_b.iter().map(|table| format!("  - {table}")));
    }
    lines.push(String::new());

    let changed_tables = table_diffs
        .iter()
        .filter(|diff| diff.has_difference())
        .collect::<Vec<_>>();
    lines.push(format!("共同表中存在差异的表 ({}):", changed_tables.len()));
    if changed_tables.is_empty() {
        lines.push("  (无)".to_string());
        return lines.join("\n");
    }

    for diff in changed_tables {
        lines.push(String::new());
        lines.push(format!("--- 表: {} ---", diff.table));
        if diff.schema_changed {
            lines.push("  [结构差异] 有".to_string());
            for line in diff.schema_detail.iter().take(MAX_SCHEMA_DIFF_LINES) {
                lines.push(format!("  {line}"));
            }
            if diff.schema_detail.len() > MAX_SCHEMA_DIFF_LINES {
                lines.push("  ... (结构差异输出已截断)".to_string());
            }
        } else {
            lines.push("  [结构差异] 无".to_string());
        }

        lines.push(format!(
            "  [行数] DB A={}, DB B={}",
            diff.row_count_a.unwrap_or_default(),
            diff.row_count_b.unwrap_or_default()
        ));

        match (diff.groups_only_in_a, diff.groups_only_in_b) {
            (Some(only_in_a), Some(only_in_b)) => {
                lines.push(format!(
                    "  [数据差异] 仅在 DB A 的分组行: {only_in_a}, 仅在 DB B 的分组行: {only_in_b}"
                ));
                if only_in_a > 0 {
                    lines.push("  [样例] 仅在 DB A 的分组行（含 __row_count__）:".to_string());
                    lines.push(format_sample_rows(&diff.sample_only_in_a));
                }
                if only_in_b > 0 {
                    lines.push("  [样例] 仅在 DB B 的分组行（含 __row_count__）:".to_string());
                    lines.push(format_sample_rows(&diff.sample_only_in_b));
                }
            }
            _ => lines.push("  [数据差异] 未比较（结构不同或启用了 --skip-data）".to_string()),
        }
    }

    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_compare_conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute("ATTACH DATABASE ':memory:' AS db_b", [])
            .unwrap();
        conn
    }

    #[test]
    fn quotes_identifiers() {
        assert_eq!(quote_ident("simple"), "\"simple\"");
        assert_eq!(quote_ident("bad\"name"), "\"bad\"\"name\"");
    }

    #[test]
    fn builds_schema_diff() {
        let diff = build_schema_diff_lines(
            "CREATE TABLE t (\n  id INTEGER\n)",
            "CREATE TABLE t (\n  id TEXT\n)",
        );
        assert!(diff.iter().any(|line| line == "--- db_a"));
        assert!(diff.iter().any(|line| line == "+++ db_b"));
        assert!(diff.iter().any(|line| line == "-  id INTEGER"));
        assert!(diff.iter().any(|line| line == "+  id TEXT"));
    }

    #[test]
    fn compare_table_uses_tolerance() {
        let conn = setup_compare_conn();
        let create_sql_main = "\
            CREATE TABLE tbl_pc_terminal_waypoints (
                region_code TEXT,
                waypoint_identifier TEXT,
                waypoint_latitude REAL,
                waypoint_longitude REAL,
                magnetic_variation REAL
            )";
        let create_sql_attached = "\
            CREATE TABLE db_b.tbl_pc_terminal_waypoints (
                region_code TEXT,
                waypoint_identifier TEXT,
                waypoint_latitude REAL,
                waypoint_longitude REAL,
                magnetic_variation REAL
            )";
        conn.execute(create_sql_main, []).unwrap();
        conn.execute(create_sql_attached, []).unwrap();

        conn.execute(
            "INSERT INTO tbl_pc_terminal_waypoints VALUES ('ZB', 'FIX01', 30.123456781, 120.987654321, 1.0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO db_b.tbl_pc_terminal_waypoints VALUES ('ZB', 'FIX01', 30.123456784, 120.987654324, 1.1)",
            [],
        )
        .unwrap();

        let diff = compare_table(&conn, "tbl_pc_terminal_waypoints", 5, false).unwrap();
        assert!(!diff.schema_changed);
        assert_eq!(diff.groups_only_in_a, Some(0));
        assert_eq!(diff.groups_only_in_b, Some(0));
        assert!(!diff.has_difference());
    }

    #[test]
    fn compare_table_uses_tolerance_for_pmdg_table_name() {
        let conn = setup_compare_conn();
        let create_sql_main = "\
            CREATE TABLE tbl_terminal_waypoints (
                region_code TEXT,
                waypoint_identifier TEXT,
                waypoint_latitude REAL,
                waypoint_longitude REAL,
                magnetic_variation REAL
            )";
        let create_sql_attached = "\
            CREATE TABLE db_b.tbl_terminal_waypoints (
                region_code TEXT,
                waypoint_identifier TEXT,
                waypoint_latitude REAL,
                waypoint_longitude REAL,
                magnetic_variation REAL
            )";
        conn.execute(create_sql_main, []).unwrap();
        conn.execute(create_sql_attached, []).unwrap();

        conn.execute(
            "INSERT INTO tbl_terminal_waypoints VALUES ('ZB', 'FIX01', 30.123456781, 120.987654321, 1.0)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO db_b.tbl_terminal_waypoints VALUES ('ZB', 'FIX01', 30.123456784, 120.987654324, 1.1)",
            [],
        )
        .unwrap();

        let diff = compare_table(&conn, "tbl_terminal_waypoints", 5, false).unwrap();
        assert!(!diff.schema_changed);
        assert_eq!(diff.groups_only_in_a, Some(0));
        assert_eq!(diff.groups_only_in_b, Some(0));
        assert!(!diff.has_difference());
    }

    #[test]
    fn table_name_match_supports_legacy_prefix() {
        assert!(table_name_matches("tbl_terminal_waypoints", "tbl_terminal_waypoints"));
        assert!(table_name_matches("tbl_pc_terminal_waypoints", "tbl_terminal_waypoints"));
        assert!(table_name_matches("tbl_db_enroute_ndbnavaids", "tbl_enroute_ndbnavaids"));
        assert!(!table_name_matches("tbl_xyz_terminal_waypoints", "tbl_terminal_waypoints"));
    }

    #[test]
    fn compare_table_detects_grouped_count_difference() {
        let conn = setup_compare_conn();
        conn.execute("CREATE TABLE sample (id INTEGER, name TEXT)", [])
            .unwrap();
        conn.execute("CREATE TABLE db_b.sample (id INTEGER, name TEXT)", [])
            .unwrap();

        conn.execute("INSERT INTO sample VALUES (1, 'A')", [])
            .unwrap();
        conn.execute("INSERT INTO sample VALUES (1, 'A')", [])
            .unwrap();
        conn.execute("INSERT INTO db_b.sample VALUES (1, 'A')", [])
            .unwrap();

        let diff = compare_table(&conn, "sample", 5, false).unwrap();
        assert_eq!(diff.groups_only_in_a, Some(1));
        assert_eq!(diff.groups_only_in_b, Some(1));
        assert!(diff.has_difference());
        assert_eq!(diff.sample_only_in_a.len(), 1);
    }

    #[test]
    fn exit_code_is_zero_by_default_even_when_diff_exists() {
        assert_eq!(exit_code_for_diff(true, false), 0);
        assert_eq!(exit_code_for_diff(false, false), 0);
        assert_eq!(exit_code_for_diff(true, true), 1);
    }
}