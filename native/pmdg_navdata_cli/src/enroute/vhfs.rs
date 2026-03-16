use crate::core::db::RustSqliteConnection;
use crate::core::magnetic::batch_get_magnetic_variations_internal;
use crate::core::parsers::parse_vhf_nav_file;
use anyhow::{anyhow, Result};
use csv::{ReaderBuilder, StringRecord, Trim};
use encoding_rs::Encoding;
use pinyin::ToPinyin;
use rusqlite::types::Value as SqlValue;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::ErrorKind;

const SQLITE_MAX_VARIABLE_NUMBER: usize = 999;
const VHFS_TABLE: &str = "tbl_vhfnavaids";

#[derive(Clone)]
struct VhfInsertRow {
    airport_identifier: Option<String>,
    area_code: String,
    dme_elevation: f64,
    dme_ident: Option<String>,
    dme_latitude: f64,
    dme_longitude: f64,
    icao_code: String,
    ilsdme_bias: Option<String>,
    navaid_class: String,
    navaid_frequency: f64,
    navaid_identifier: String,
    navaid_latitude: f64,
    navaid_longitude: f64,
    navaid_name: String,
    range: String,
    station_declination: Option<i64>,
    id: String,
}

fn area_code_for_icao(icao_code: &str) -> &'static str {
    match icao_code {
        "VH" => "PAC",
        _ => "EEU",
    }
}

fn build_insert_sql() -> &'static str {
    "INSERT OR IGNORE INTO tbl_vhfnavaids (area_code, airport_identifier, icao_code, vor_identifier, vor_name, vor_frequency, navaid_class, vor_latitude, vor_longitude, dme_ident, dme_latitude, dme_longitude, dme_elevation, ilsdme_bias, range, station_declination, magnetic_variation, id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
}

fn python_round(value: f64) -> i64 {
    let rounded = value.round();
    if (value - rounded).abs() != 0.5 {
        return rounded as i64;
    }

    let lower = value.floor() as i64;
    let upper = value.ceil() as i64;
    if lower % 2 == 0 {
        lower
    } else if upper % 2 == 0 {
        upper
    } else {
        rounded as i64
    }
}

fn to_upper_pinyin(value: &str) -> String {
    let mut out = String::new();
    for ch in value.chars() {
        if ch.is_whitespace() {
            continue;
        }
        if let Some(pinyin) = ch.to_pinyin() {
            out.push_str(pinyin.plain());
        } else {
            out.extend(ch.to_uppercase());
        }
    }
    out.to_uppercase()
}

fn required_index(headers: &StringRecord, column: &str) -> Result<usize> {
    headers
        .iter()
        .position(|value| value == column)
        .ok_or_else(|| anyhow!("required navaid CSV column missing: {}", column))
}

fn optional_index(headers: &StringRecord, column: &str) -> Option<usize> {
    headers.iter().position(|value| value == column)
}

fn optional_string(record: &StringRecord, index: Option<usize>) -> Option<String> {
    index
        .and_then(|idx| record.get(idx))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn decode_gb18030_bytes(bytes: &[u8]) -> Result<String> {
    let encoding = Encoding::for_label(b"gb18030")
        .ok_or_else(|| anyhow!("gb18030 encoding is unavailable"))?;
    let (text, _, _) = encoding.decode(bytes);
    Ok(text.into_owned())
}

fn load_navaid_mapping_from_csv(
    csv_path: &str,
    navaid_mapping: &mut HashMap<String, String>,
) -> Result<()> {
    let bytes = match fs::read(csv_path) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(()),
        Err(err) => {
            return Err(anyhow!("failed to read {}: {}", csv_path, err));
        }
    };
    let content = decode_gb18030_bytes(&bytes)?;

    let mut reader = ReaderBuilder::new()
        .trim(Trim::All)
        .flexible(true)
        .from_reader(content.as_bytes());
    let headers = reader
        .headers()
        .map_err(|err| anyhow!("failed to read navaid CSV headers: {}", err))?
        .clone();
    let code_id_idx = required_index(&headers, "CODE_ID")?;
    let txt_name_idx = optional_index(&headers, "TXT_NAME");

    for record in reader.records() {
        let record = record.map_err(|err| anyhow!("failed to parse navaid CSV row: {}", err))?;
        let Some(code_id) = record
            .get(code_id_idx)
            .map(str::trim)
            .filter(|value| !value.is_empty())
        else {
            continue;
        };
        let Some(txt_name) = optional_string(&record, txt_name_idx) else {
            continue;
        };
        let py_name = to_upper_pinyin(&txt_name);
        navaid_mapping.insert(code_id.to_string(), py_name);
    }

    Ok(())
}

fn fetch_existing_pairs_for_keys(
    conn: &RustSqliteConnection,
    table_name: &str,
    keys: &[(String, String)],
    batch_size: usize,
) -> Result<HashSet<(String, String)>> {
    let mut pairs = HashSet::new();
    if keys.is_empty() {
        return Ok(pairs);
    }

    let effective_batch = batch_size
        .max(1)
        .min((SQLITE_MAX_VARIABLE_NUMBER / 2).max(1));
    for chunk in keys.chunks(effective_batch) {
        let placeholders = vec!["(?, ?)"; chunk.len()].join(",");
        let query = format!(
            "SELECT vor_identifier, icao_code FROM {} WHERE (vor_identifier, icao_code) IN ({})",
            table_name, placeholders
        );
        let params = chunk
            .iter()
            .flat_map(|(navaid_identifier, icao_code)| {
                [
                    SqlValue::Text(navaid_identifier.clone()),
                    SqlValue::Text(icao_code.clone()),
                ]
            })
            .collect::<Vec<_>>();
        conn.query_each_native(&query, &params, |row| {
            let navaid_identifier: String = row.get(0)?;
            let icao_code: String = row.get(1)?;
            pairs.insert((navaid_identifier, icao_code));
            Ok(())
        })
        .map_err(sqlite_error)?;
    }

    Ok(pairs)
}

fn bind_vhf_row(
    stmt: &mut rusqlite::Statement<'_>,
    row: &VhfInsertRow,
    magnetic_variation: f64,
) -> rusqlite::Result<()> {
    stmt.raw_bind_parameter(1, row.area_code.as_str())?;
    match &row.airport_identifier {
        Some(v) => stmt.raw_bind_parameter(2, v.as_str())?,
        None => stmt.raw_bind_parameter(2, rusqlite::types::Null)?,
    }
    stmt.raw_bind_parameter(3, row.icao_code.as_str())?;
    stmt.raw_bind_parameter(4, row.navaid_identifier.as_str())?;
    stmt.raw_bind_parameter(5, row.navaid_name.as_str())?;
    stmt.raw_bind_parameter(6, row.navaid_frequency)?;
    stmt.raw_bind_parameter(7, row.navaid_class.as_str())?;
    stmt.raw_bind_parameter(8, row.navaid_latitude)?;
    stmt.raw_bind_parameter(9, row.navaid_longitude)?;
    match &row.dme_ident {
        Some(v) => stmt.raw_bind_parameter(10, v.as_str())?,
        None => stmt.raw_bind_parameter(10, rusqlite::types::Null)?,
    }
    stmt.raw_bind_parameter(11, row.dme_latitude)?;
    stmt.raw_bind_parameter(12, row.dme_longitude)?;
    stmt.raw_bind_parameter(13, row.dme_elevation)?;
    match &row.ilsdme_bias {
        Some(v) => stmt.raw_bind_parameter(14, v.as_str())?,
        None => stmt.raw_bind_parameter(14, rusqlite::types::Null)?,
    }
    stmt.raw_bind_parameter(15, row.range.as_str())?;
    match row.station_declination {
        Some(v) => stmt.raw_bind_parameter(16, v)?,
        None => stmt.raw_bind_parameter(16, rusqlite::types::Null)?,
    }
    stmt.raw_bind_parameter(17, magnetic_variation)?;
    stmt.raw_bind_parameter(18, row.id.as_str())?;
    stmt.raw_execute()?;
    Ok(())
}

fn insert_rows(
    conn: &RustSqliteConnection,
    rows: &[VhfInsertRow],
    magnetic_variations: &[f64],
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }

    let sql = build_insert_sql();
    conn.with_connection_native(|raw_conn| {
        let batch = 500;
        for start in (0..rows.len()).step_by(batch) {
            let end = (start + batch).min(rows.len());
            let tx = raw_conn.unchecked_transaction()?;
            {
                let mut stmt = tx.prepare(&sql)?;
                for (offset, row) in rows[start..end].iter().enumerate() {
                    let row_index = start + offset;
                    bind_vhf_row(&mut stmt, row, magnetic_variations[row_index])?;
                }
            }
            tx.commit()?;
        }
        Ok(())
    })?;
    Ok(())
}

pub(crate) fn process_vhfs_to_db(
    file_path: &str,
    vor_csv_path: &str,
    ndb_csv_path: &str,
    conn: &RustSqliteConnection,
) -> Result<usize> {
    conn.execute_statement_native(
            "\n            CREATE TABLE IF NOT EXISTS tbl_vhfnavaids (\n                airport_identifier TEXT,\n                area_code TEXT,\n                icao_code TEXT,\n                vor_identifier TEXT,\n                vor_name TEXT,\n                vor_frequency REAL,\n                navaid_class TEXT,\n                vor_latitude REAL,\n                vor_longitude REAL,\n                dme_ident TEXT,\n                dme_latitude REAL,\n                dme_longitude REAL,\n                dme_elevation REAL,\n                ilsdme_bias TEXT,\n                range TEXT,\n                station_declination REAL,\n                magnetic_variation REAL,\n                id TEXT\n            )\n        ",
            &[],
        )
        .map_err(sqlite_error)?;
    let parsed_rows = parse_vhf_nav_file(file_path)
        .map_err(|err| anyhow!("parse_vhf_nav_file failed: {}", err))?;
    let unique_pairs = parsed_rows
        .iter()
        .map(
            |(_, icao_code, navaid_identifier, _, _, _, _, _, _, _, _)| {
                (navaid_identifier.clone(), icao_code.clone())
            },
        )
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let existing_pairs = fetch_existing_pairs_for_keys(conn, VHFS_TABLE, &unique_pairs, 500)
        .map_err(|err| anyhow!("fetch_existing_pairs_for_keys failed: {}", err))?;

    let mut navaid_mapping = HashMap::new();
    load_navaid_mapping_from_csv(vor_csv_path, &mut navaid_mapping)
        .map_err(|err| anyhow!("load_navaid_mapping_from_csv failed for VOR.csv: {}", err))?;
    load_navaid_mapping_from_csv(ndb_csv_path, &mut navaid_mapping)
        .map_err(|err| anyhow!("load_navaid_mapping_from_csv failed for NDB.csv: {}", err))?;

    let mut seen_keys = HashSet::new();
    let mut coordinates = Vec::new();
    let mut pending_rows = Vec::new();

    for (
        airport_identifier,
        icao_code,
        navaid_identifier,
        navaid_name,
        navaid_frequency,
        navaid_class,
        navaid_latitude,
        navaid_longitude,
        dme_elevation,
        nav_range,
        has_dme_ident,
    ) in parsed_rows
    {
        let key = (navaid_identifier.clone(), icao_code.clone());
        if existing_pairs.contains(&key) || !seen_keys.insert(key) {
            continue;
        }

        coordinates.push((navaid_latitude, navaid_longitude));
        pending_rows.push((
            airport_identifier,
            icao_code,
            navaid_identifier,
            navaid_name,
            navaid_frequency,
            navaid_class,
            navaid_latitude,
            navaid_longitude,
            dme_elevation,
            nav_range,
            has_dme_ident,
        ));
    }

    if pending_rows.is_empty() {
        return Ok(0);
    }

    let magnetic_variations = batch_get_magnetic_variations_internal(&coordinates)
        .map_err(|err| anyhow!("batch_get_magnetic_variations_internal failed: {}", err))?;

    let rows: Vec<VhfInsertRow> = pending_rows
        .into_iter()
        .enumerate()
        .map(
            |(
                index,
                (
                    airport_identifier,
                    icao_code,
                    navaid_identifier,
                    navaid_name,
                    navaid_frequency,
                    navaid_class,
                    navaid_latitude,
                    navaid_longitude,
                    dme_elevation,
                    nav_range,
                    has_dme_ident,
                ),
            )| {
                let current_magnetic_variation =
                    magnetic_variations.get(index).copied().unwrap_or(0.0);
                let station_declination = if navaid_class == "VDHW " {
                    Some(python_round(current_magnetic_variation))
                } else {
                    None
                };
                let id = format!("{}{}", icao_code, navaid_identifier);
                VhfInsertRow {
                    airport_identifier,
                    area_code: area_code_for_icao(&icao_code).to_string(),
                    dme_elevation,
                    dme_ident: if has_dme_ident {
                        Some(navaid_identifier.clone())
                    } else {
                        None
                    },
                    dme_latitude: navaid_latitude,
                    dme_longitude: navaid_longitude,
                    icao_code,
                    ilsdme_bias: None,
                    navaid_class,
                    navaid_frequency,
                    navaid_identifier: navaid_identifier.clone(),
                    navaid_latitude,
                    navaid_longitude,
                    navaid_name: navaid_name
                        .or_else(|| navaid_mapping.get(&navaid_identifier).cloned())
                        .unwrap_or_else(|| navaid_identifier.clone()),
                    range: nav_range,
                    station_declination,
                    id,
                }
            },
        )
        .collect();

    insert_rows(conn, &rows, &magnetic_variations)
        .map_err(|err| anyhow!("insert_rows failed: {}", err))?;
    Ok(rows.len())
}

fn sqlite_error(err: rusqlite::Error) -> anyhow::Error {
    anyhow!(err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_vhf_names_to_upper_pinyin_without_python() {
        assert_eq!(to_upper_pinyin("广州"), "GUANGZHOU");
        assert_eq!(to_upper_pinyin("vor"), "VOR");
        assert_eq!(to_upper_pinyin("VOR 广州 112.3"), "VORGUANGZHOU112.3");
    }
}
