use crate::core::db::{join_quoted_sqlite_identifiers, quote_sqlite_identifier, RustSqliteConnection};
use crate::core::magnetic::batch_get_magnetic_variations_internal;
use crate::core::parsers::parse_ndb_nav_file;
use anyhow::{anyhow, Result};
use rusqlite::types::Null;
use rusqlite::types::Value as SqlValue;
use rusqlite::Statement;
use std::collections::HashSet;

const SQLITE_MAX_VARIABLE_NUMBER: usize = 999;
const ENROUTE_NDBS_TABLE: &str = "tbl_enroute_ndbnavaids";

#[derive(Clone)]
struct NdbInsertRow {
    area_code: String,
    continent: String,
    country: String,
    datum_code: String,
    icao_code: String,
    navaid_class: String,
    navaid_frequency: f64,
    navaid_identifier: String,
    navaid_latitude: f64,
    navaid_longitude: f64,
    navaid_name: String,
    range: f64,
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
            "SELECT ndb_identifier, icao_code FROM {} WHERE (ndb_identifier, icao_code) IN ({})",
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

fn bind_ndb_row_for_columns(
    stmt: &mut Statement<'_>,
    row: &NdbInsertRow,
    magnetic_variation: Option<f64>,
    columns: &[String],
) -> rusqlite::Result<()> {
    for (index, column) in columns.iter().enumerate() {
        let parameter_index = index + 1;
        match column.as_str() {
            "area_code" => stmt.raw_bind_parameter(parameter_index, row.area_code.as_str())?,
            "continent" => stmt.raw_bind_parameter(parameter_index, row.continent.as_str())?,
            "country" => stmt.raw_bind_parameter(parameter_index, row.country.as_str())?,
            "datum_code" => stmt.raw_bind_parameter(parameter_index, row.datum_code.as_str())?,
            "icao_code" => stmt.raw_bind_parameter(parameter_index, row.icao_code.as_str())?,
            "magnetic_variation" => {
                if let Some(value) = magnetic_variation {
                    stmt.raw_bind_parameter(parameter_index, value)?
                } else {
                    stmt.raw_bind_parameter(parameter_index, Null)?
                }
            }
            "navaid_class" => stmt.raw_bind_parameter(parameter_index, row.navaid_class.as_str())?,
            "ndb_frequency" | "navaid_frequency" => stmt.raw_bind_parameter(parameter_index, row.navaid_frequency)?,
            "ndb_identifier" | "navaid_identifier" => {
                stmt.raw_bind_parameter(parameter_index, row.navaid_identifier.as_str())?
            }
            "ndb_latitude" | "navaid_latitude" => stmt.raw_bind_parameter(parameter_index, row.navaid_latitude)?,
            "ndb_longitude" | "navaid_longitude" => stmt.raw_bind_parameter(parameter_index, row.navaid_longitude)?,
            "ndb_name" | "navaid_name" => stmt.raw_bind_parameter(parameter_index, row.navaid_name.as_str())?,
            "range" => stmt.raw_bind_parameter(parameter_index, row.range)?,
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
    rows: &[NdbInsertRow],
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
                    bind_ndb_row_for_columns(
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

pub(crate) fn process_ndbs_to_db(
    dat_file_path: &str,
    conn: &RustSqliteConnection,
) -> Result<usize> {
    conn.execute_statement_native(
            "\n        CREATE TABLE IF NOT EXISTS tbl_enroute_ndbnavaids (\n            area_code TEXT,\n            icao_code TEXT,\n            ndb_identifier TEXT,\n            ndb_name TEXT,\n            ndb_frequency REAL,\n            navaid_class TEXT,\n            ndb_latitude REAL,\n            ndb_longitude REAL,\n            range REAL,\n            id TEXT\n        )\n        ",
            &[],
        )
        .map_err(sqlite_error)?;
    let columns = conn.get_table_columns_native(ENROUTE_NDBS_TABLE)?;

    let parsed_rows = parse_ndb_nav_file(dat_file_path)
        .map_err(|err| anyhow!("parse_ndb_nav_file failed: {}", err))?;
    let unique_pairs = parsed_rows
        .iter()
        .map(|(icao_code, navaid_identifier, _, _, _, _, _)| {
            (navaid_identifier.clone(), icao_code.clone())
        })
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let existing_pairs = fetch_existing_pairs_for_keys(conn, ENROUTE_NDBS_TABLE, &unique_pairs, 500)
        .map_err(|err| anyhow!("fetch_existing_pairs_for_keys failed: {}", err))?;

    let mut pending_rows: Vec<(String, String, String, f64, f64, f64, f64)> = Vec::new();
    let mut coordinates = Vec::new();
    for (
        icao_code,
        navaid_identifier,
        navaid_name,
        navaid_frequency,
        navaid_latitude,
        navaid_longitude,
        ndb_range,
    ) in parsed_rows
    {
        if existing_pairs.contains(&(navaid_identifier.clone(), icao_code.clone())) {
            continue;
        }

        coordinates.push((navaid_latitude, navaid_longitude));
        pending_rows.push((
            icao_code,
            navaid_identifier,
            navaid_name,
            navaid_frequency,
            navaid_latitude,
            navaid_longitude,
            ndb_range,
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

    let rows: Vec<NdbInsertRow> = pending_rows
        .into_iter()
        .map(
            |(
                icao_code,
                navaid_identifier,
                navaid_name,
                navaid_frequency,
                navaid_latitude,
                navaid_longitude,
                ndb_range,
            )| NdbInsertRow {
                area_code: area_code_for_icao(&icao_code).to_string(),
                continent: "ASIA".to_string(),
                country: "CHINA".to_string(),
                datum_code: "WGE".to_string(),
                id: format!("{}{}", icao_code, navaid_identifier),
                icao_code,
                navaid_class: "H W".to_string(),
                navaid_frequency,
                navaid_identifier,
                navaid_latitude,
                navaid_longitude,
                navaid_name,
                range: ndb_range,
            },
        )
        .collect();

    insert_rows(conn, ENROUTE_NDBS_TABLE, &columns, &rows, magnetic_variations.as_deref())
        .map_err(|err| anyhow!("insert_rows failed: {}", err))?;
    Ok(rows.len())
}

fn sqlite_error(err: rusqlite::Error) -> anyhow::Error {
    anyhow!(err.to_string())
}
