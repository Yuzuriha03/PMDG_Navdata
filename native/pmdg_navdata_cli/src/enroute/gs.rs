use crate::core::db::{join_quoted_sqlite_identifiers, quote_sqlite_identifier, RustSqliteConnection};
use crate::core::magnetic::batch_get_magnetic_variations_internal;
use anyhow::{anyhow, Result};
use rusqlite::types::Null;
use rusqlite::types::Value as SqlValue;
use rusqlite::Statement;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader};

const SQLITE_MAX_VARIABLE_NUMBER: usize = 999;
const DAT_READER_CAPACITY: usize = 256 * 1024;
const LOCALIZERS_TABLE: &str = "tbl_localizers_glideslopes";

#[derive(Clone)]
struct LocalizerInfo {
    llz_latitude: f64,
    llz_longitude: f64,
    ils_cat: String,
}

#[derive(Clone)]
struct GsInsertRow {
    airport_identifier: String,
    area_code: String,
    gs_angle: f64,
    gs_elevation: i64,
    gs_latitude: f64,
    gs_longitude: f64,
    icao_code: String,
    ils_mls_gls_category: String,
    llz_bearing: f64,
    llz_frequency: f64,
    llz_identifier: String,
    llz_latitude: f64,
    llz_longitude: f64,
    llz_width: f64,
    runway_identifier: String,
    station_declination: f64,
    id: String,
}

fn area_code_for_icao(icao_code: &str) -> &'static str {
    match icao_code {
        "VH" => "PAC",
        _ => "EEU",
    }
}

fn build_insert_sql(table_name: &str, columns: &[String]) -> String {
    let placeholders = vec!["?"; columns.len()].join(", ");
    format!(
        "INSERT OR IGNORE INTO {} ({}) VALUES ({})",
        quote_sqlite_identifier(table_name),
        join_quoted_sqlite_identifiers(columns),
        placeholders,
    )
}

fn open_text_reader(file_path: &str) -> Result<BufReader<File>> {
    let file =
        File::open(file_path).map_err(|err| anyhow!("failed to open {}: {}", file_path, err))?;
    Ok(BufReader::with_capacity(DAT_READER_CAPACITY, file))
}

fn classify_ils_category(ils_cat: &str) -> i64 {
    let lower = ils_cat.to_lowercase();
    if !lower.contains("cat") {
        return 1;
    }
    if lower.contains("iii") {
        3
    } else if lower.contains("ii") {
        2
    } else {
        1
    }
}

fn parse_frequency(raw: &str) -> Option<f64> {
    if raw.len() < 4 {
        return None;
    }
    let (head, tail) = raw.split_at(3);
    format!("{}.{}", head, tail).parse().ok()
}

fn parse_localizers(file_path: &str) -> Result<HashMap<(String, String, String), LocalizerInfo>> {
    let mut localizers = HashMap::new();
    let mut reader = open_text_reader(file_path)?;
    let mut line = String::new();

    loop {
        line.clear();
        if reader.read_line(&mut line)? == 0 {
            break;
        }

        let trimmed = line.trim_end_matches(|ch| matches!(ch, '\r' | '\n'));
        let parts: Vec<&str> = trimmed.split_whitespace().collect();
        if parts.len() < 12 || parts[0] != "4" {
            continue;
        }

        let (Ok(llz_latitude), Ok(llz_longitude)) =
            (parts[1].parse::<f64>(), parts[2].parse::<f64>())
        else {
            continue;
        };

        let key = (
            parts[4].to_string(),
            parts[7].to_string(),
            parts[8].to_string(),
        );
        localizers.insert(
            key,
            LocalizerInfo {
                llz_latitude,
                llz_longitude,
                ils_cat: parts.get(11).copied().unwrap_or_default().to_string(),
            },
        );
    }

    Ok(localizers)
}

fn fetch_existing_keys_for_rows(
    conn: &RustSqliteConnection,
    table_name: &str,
    keys: &[(String, String, String)],
    batch_size: usize,
) -> Result<HashSet<(String, String, String)>> {
    let mut existing = HashSet::new();
    if keys.is_empty() {
        return Ok(existing);
    }

    let effective_batch = batch_size
        .max(1)
        .min((SQLITE_MAX_VARIABLE_NUMBER / 3).max(1));
    for chunk in keys.chunks(effective_batch) {
        let placeholders = vec!["(?, ?, ?)"; chunk.len()].join(",");
        let query = format!(
            "SELECT airport_identifier, runway_identifier, llz_identifier FROM {} WHERE (airport_identifier, runway_identifier, llz_identifier) IN ({})",
            quote_sqlite_identifier(table_name),
            placeholders
        );
        let params = chunk
            .iter()
            .flat_map(|(airport_identifier, runway_identifier, llz_identifier)| {
                [
                    SqlValue::Text(airport_identifier.clone()),
                    SqlValue::Text(runway_identifier.clone()),
                    SqlValue::Text(llz_identifier.clone()),
                ]
            })
            .collect::<Vec<_>>();
        conn.query_each_native(&query, &params, |row| {
            let airport_identifier: String = row.get(0)?;
            let runway_identifier: String = row.get(1)?;
            let llz_identifier: String = row.get(2)?;
            existing.insert((airport_identifier, runway_identifier, llz_identifier));
            Ok(())
        })
        .map_err(sqlite_error)?;
    }

    Ok(existing)
}

fn bind_gs_row_for_columns(
    stmt: &mut Statement<'_>,
    row: &GsInsertRow,
    columns: &[String],
) -> rusqlite::Result<()> {
    for (index, column) in columns.iter().enumerate() {
        let parameter_index = index + 1;
        match column.as_str() {
            "airport_identifier" => stmt.raw_bind_parameter(parameter_index, row.airport_identifier.as_str())?,
            "area_code" => stmt.raw_bind_parameter(parameter_index, row.area_code.as_str())?,
            "gs_angle" => stmt.raw_bind_parameter(parameter_index, row.gs_angle)?,
            "gs_elevation" => stmt.raw_bind_parameter(parameter_index, row.gs_elevation)?,
            "gs_latitude" => stmt.raw_bind_parameter(parameter_index, row.gs_latitude)?,
            "gs_longitude" => stmt.raw_bind_parameter(parameter_index, row.gs_longitude)?,
            "icao_code" => stmt.raw_bind_parameter(parameter_index, row.icao_code.as_str())?,
            "ils_mls_gls_category" => {
                stmt.raw_bind_parameter(parameter_index, row.ils_mls_gls_category.as_str())?
            }
            "llz_bearing" => stmt.raw_bind_parameter(parameter_index, row.llz_bearing)?,
            "llz_frequency" => stmt.raw_bind_parameter(parameter_index, row.llz_frequency)?,
            "llz_identifier" => stmt.raw_bind_parameter(parameter_index, row.llz_identifier.as_str())?,
            "llz_latitude" => stmt.raw_bind_parameter(parameter_index, row.llz_latitude)?,
            "llz_longitude" => stmt.raw_bind_parameter(parameter_index, row.llz_longitude)?,
            "llz_width" => stmt.raw_bind_parameter(parameter_index, row.llz_width)?,
            "runway_identifier" => stmt.raw_bind_parameter(parameter_index, row.runway_identifier.as_str())?,
            "station_declination" => stmt.raw_bind_parameter(parameter_index, row.station_declination)?,
            "id" => stmt.raw_bind_parameter(parameter_index, row.id.as_str())?,
            _ => stmt.raw_bind_parameter(parameter_index, Null)?,
        }
    }
    stmt.raw_execute()?;
    Ok(())
}

fn insert_rows(
    conn: &RustSqliteConnection,
    table_name: &str,
    columns: &[String],
    rows: &[GsInsertRow],
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }

    let sql = build_insert_sql(table_name, columns);
    conn.with_connection_native(|raw_conn| {
        let batch = 500;
        for start in (0..rows.len()).step_by(batch) {
            let end = (start + batch).min(rows.len());
            let tx = raw_conn.unchecked_transaction()?;
            {
                let mut stmt = tx.prepare(&sql)?;
                for row in rows.iter().take(end).skip(start) {
                    bind_gs_row_for_columns(&mut stmt, row, columns)?;
                }
            }
            tx.commit()?;
        }
        Ok(())
    })?;
    Ok(())
}

fn parse_gs_rows(file_path: &str) -> Result<Vec<GsInsertRow>> {
    let localizers = parse_localizers(file_path)?;

    let mut pending_rows = Vec::new();
    let mut coordinates = Vec::new();
    let mut reader = open_text_reader(file_path)?;
    let mut line = String::new();

    loop {
        line.clear();
        if reader.read_line(&mut line)? == 0 {
            break;
        }

        let trimmed = line.trim_end_matches(|ch| matches!(ch, '\r' | '\n'));
        let parts: Vec<&str> = trimmed.split_whitespace().collect();
        if parts.len() < 12 || parts[0] != "6" {
            continue;
        }

        let airport_identifier = parts[8].to_string();

        let key = (
            parts[4].to_string(),
            parts[7].to_string(),
            airport_identifier.clone(),
        );
        let Some(localizer) = localizers.get(&key) else {
            continue;
        };

        let nav_info = parts[6];
        if nav_info.len() < 4 {
            continue;
        }

        let (
            Ok(gs_elevation),
            Ok(gs_latitude),
            Ok(gs_longitude),
            Ok(gs_angle_raw),
            Ok(llz_truebearing_raw),
        ) = (
            parts[3].parse::<i64>(),
            parts[1].parse::<f64>(),
            parts[2].parse::<f64>(),
            nav_info[..3].parse::<f64>(),
            nav_info[3..].parse::<f64>(),
        )
        else {
            continue;
        };

        let Some(llz_frequency) = parse_frequency(parts[4]) else {
            continue;
        };

        coordinates.push((localizer.llz_latitude, localizer.llz_longitude));
        pending_rows.push((
            airport_identifier,
            gs_angle_raw / 100.0,
            gs_elevation,
            gs_latitude,
            gs_longitude,
            parts[9].to_string(),
            classify_ils_category(&localizer.ils_cat),
            llz_frequency,
            parts[7].to_string(),
            localizer.llz_latitude,
            localizer.llz_longitude,
            llz_truebearing_raw,
            format!("RW{}", parts[10]),
        ));
    }

    if pending_rows.is_empty() {
        return Ok(Vec::new());
    }

    let declinations = batch_get_magnetic_variations_internal(&coordinates)?;
    let mut out = Vec::with_capacity(pending_rows.len());

    for (row, station_declination) in pending_rows.into_iter().zip(declinations) {
        let llz_bearing = (row.11 - station_declination).rem_euclid(360.0);
        let id = format!("{}{}{}", row.0, row.5, row.8);
        out.push(GsInsertRow {
            airport_identifier: row.0,
            area_code: area_code_for_icao(&row.5).to_string(),
            gs_angle: row.1,
            gs_elevation: row.2,
            gs_latitude: row.3,
            gs_longitude: row.4,
            icao_code: row.5,
            ils_mls_gls_category: row.6.to_string(),
            llz_bearing,
            llz_frequency: row.7,
            llz_identifier: row.8,
            llz_latitude: row.9,
            llz_longitude: row.10,
            llz_width: 3.0,
            runway_identifier: row.12,
            station_declination,
            id,
        });
    }

    Ok(out)
}

pub(crate) fn process_ils_gs_to_db(file_path: &str, conn: &RustSqliteConnection) -> Result<usize> {
    conn.execute_statement_native(
            "\n            CREATE TABLE IF NOT EXISTS tbl_localizers_glideslopes (\n                area_code TEXT,\n                icao_code TEXT,\n                airport_identifier TEXT,\n                runway_identifier TEXT,\n                llz_identifier TEXT,\n                llz_latitude REAL,\n                llz_longitude REAL,\n                llz_frequency REAL,\n                llz_bearing REAL,\n                llz_width REAL,\n                ils_mls_gls_category TEXT,\n                gs_latitude REAL,\n                gs_longitude REAL,\n                gs_angle REAL,\n                gs_elevation INTEGER,\n                station_declination REAL,\n                id TEXT\n            )\n        ",
            &[],
        )
        .map_err(sqlite_error)?;
    let columns = conn.get_table_columns_native(LOCALIZERS_TABLE)?;

    let rows = parse_gs_rows(file_path).map_err(|err| anyhow!("parse_gs_rows failed: {}", err))?;
    let unique_keys = rows
        .iter()
        .map(|row| {
            (
                row.airport_identifier.clone(),
                row.runway_identifier.clone(),
                row.llz_identifier.clone(),
            )
        })
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let existing_keys = fetch_existing_keys_for_rows(conn, LOCALIZERS_TABLE, &unique_keys, 300)
        .map_err(|err| anyhow!("fetch_existing_keys_for_rows failed: {}", err))?;

    let new_rows: Vec<GsInsertRow> = rows
        .into_iter()
        .filter(|row| {
            !existing_keys.contains(&(
                row.airport_identifier.clone(),
                row.runway_identifier.clone(),
                row.llz_identifier.clone(),
            ))
        })
        .collect();

    insert_rows(conn, LOCALIZERS_TABLE, &columns, &new_rows)
        .map_err(|err| anyhow!("insert_rows failed: {}", err))?;
    Ok(new_rows.len())
}

fn sqlite_error(err: rusqlite::Error) -> anyhow::Error {
    anyhow!(err.to_string())
}
