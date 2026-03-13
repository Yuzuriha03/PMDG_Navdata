use crate::core::db::{join_quoted_sqlite_identifiers, quote_sqlite_identifier, RustSqliteConnection};
use crate::core::magnetic::batch_get_magnetic_variations_internal;
use crate::core::parsers::parse_vhf_nav_file;
use anyhow::{anyhow, Result};
use csv::{ReaderBuilder, StringRecord, Trim};
use encoding_rs::Encoding;
use pinyin::ToPinyin;
use rusqlite::types::Null;
use rusqlite::types::Value as SqlValue;
use rusqlite::Statement;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::ErrorKind;

const SQLITE_MAX_VARIABLE_NUMBER: usize = 999;
const VHFS_TABLE: &str = "tbl_vhfnavaids";

#[derive(Clone)]
struct VhfInsertRow {
    airport_identifier: Option<String>,
    area_code: String,
    continent: String,
    country: String,
    datum_code: String,
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

fn build_insert_sql(table_name: &str, columns: &[String]) -> String {
    let placeholders = vec!["?"; columns.len()].join(", ");
    format!(
        "INSERT OR IGNORE INTO {} ({}) VALUES ({})",
        quote_sqlite_identifier(table_name),
        join_quoted_sqlite_identifiers(columns),
        placeholders,
    )
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
            quote_sqlite_identifier(table_name),
            placeholders
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

fn bind_vhf_row_for_columns(
    stmt: &mut Statement<'_>,
    row: &VhfInsertRow,
    magnetic_variation: Option<f64>,
    columns: &[String],
) -> rusqlite::Result<()> {
    for (index, column) in columns.iter().enumerate() {
        let parameter_index = index + 1;
        match column.as_str() {
            "airport_identifier" => match &row.airport_identifier {
                Some(v) => stmt.raw_bind_parameter(parameter_index, v.as_str())?,
                None => stmt.raw_bind_parameter(parameter_index, Null)?,
            },
            "area_code" => stmt.raw_bind_parameter(parameter_index, row.area_code.as_str())?,
            "continent" => stmt.raw_bind_parameter(parameter_index, row.continent.as_str())?,
            "country" => stmt.raw_bind_parameter(parameter_index, row.country.as_str())?,
            "datum_code" => stmt.raw_bind_parameter(parameter_index, row.datum_code.as_str())?,
            "dme_elevation" => stmt.raw_bind_parameter(parameter_index, row.dme_elevation)?,
            "dme_ident" => match &row.dme_ident {
                Some(v) => stmt.raw_bind_parameter(parameter_index, v.as_str())?,
                None => stmt.raw_bind_parameter(parameter_index, Null)?,
            },
            "dme_latitude" => stmt.raw_bind_parameter(parameter_index, row.dme_latitude)?,
            "dme_longitude" => stmt.raw_bind_parameter(parameter_index, row.dme_longitude)?,
            "icao_code" => stmt.raw_bind_parameter(parameter_index, row.icao_code.as_str())?,
            "ilsdme_bias" => match &row.ilsdme_bias {
                Some(v) => stmt.raw_bind_parameter(parameter_index, v.as_str())?,
                None => stmt.raw_bind_parameter(parameter_index, Null)?,
            },
            "magnetic_variation" => {
                if let Some(value) = magnetic_variation {
                    stmt.raw_bind_parameter(parameter_index, value)?
                } else {
                    stmt.raw_bind_parameter(parameter_index, Null)?
                }
            }
            "navaid_class" => stmt.raw_bind_parameter(parameter_index, row.navaid_class.as_str())?,
            "vor_frequency" | "navaid_frequency" => stmt.raw_bind_parameter(parameter_index, row.navaid_frequency)?,
            "vor_identifier" | "navaid_identifier" => {
                stmt.raw_bind_parameter(parameter_index, row.navaid_identifier.as_str())?
            }
            "vor_latitude" | "navaid_latitude" => stmt.raw_bind_parameter(parameter_index, row.navaid_latitude)?,
            "vor_longitude" | "navaid_longitude" => stmt.raw_bind_parameter(parameter_index, row.navaid_longitude)?,
            "vor_name" | "navaid_name" => stmt.raw_bind_parameter(parameter_index, row.navaid_name.as_str())?,
            "range" => stmt.raw_bind_parameter(parameter_index, row.range.as_str())?,
            "station_declination" => match row.station_declination {
                Some(v) => stmt.raw_bind_parameter(parameter_index, v)?,
                None => stmt.raw_bind_parameter(parameter_index, Null)?,
            },
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
    rows: &[VhfInsertRow],
    magnetic_variations: Option<&[f64]>,
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
                for (offset, row) in rows[start..end].iter().enumerate() {
                    let row_index = start + offset;
                    bind_vhf_row_for_columns(
                        &mut stmt,
                        row,
                        magnetic_variations.map(|values| values[row_index]),
                        columns,
                    )?;
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
    let columns = conn.get_table_columns_native(VHFS_TABLE)?;

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

    let magnetic_variations = if columns.iter().any(|column| column == "magnetic_variation") {
        Some(
            batch_get_magnetic_variations_internal(&coordinates)
                .map_err(|err| anyhow!("batch_get_magnetic_variations_internal failed: {}", err))?,
        )
    } else {
        None
    };

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
                let current_magnetic_variation = magnetic_variations
                    .as_ref()
                    .and_then(|values| values.get(index).copied())
                    .unwrap_or(0.0);
                let station_declination = if navaid_class == "VDHW " && magnetic_variations.is_some() {
                    Some(python_round(current_magnetic_variation))
                } else {
                    None
                };
                let id = format!("{}{}", icao_code, navaid_identifier);
                VhfInsertRow {
                    airport_identifier,
                    area_code: area_code_for_icao(&icao_code).to_string(),
                    continent: "ASIA".to_string(),
                    country: "CHINA".to_string(),
                    datum_code: "WGE".to_string(),
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

    insert_rows(conn, VHFS_TABLE, &columns, &rows, magnetic_variations.as_deref())
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
