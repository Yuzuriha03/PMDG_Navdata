use crate::core::db::{join_quoted_sqlite_identifiers, quote_sqlite_identifier, RustSqliteConnection};
use crate::core::magnetic::batch_get_magnetic_variations_internal;
use crate::core::parsers::parse_enroute_waypoints_file;
use anyhow::{anyhow, Result};
use rusqlite::types::Null;
use rusqlite::types::Value as SqlValue;
use rusqlite::Statement;
use std::collections::HashSet;

const SQLITE_MAX_VARIABLE_NUMBER: usize = 999;
const ENROUTE_WAYPOINTS_TABLE: &str = "tbl_enroute_waypoints";

#[derive(Clone)]
struct EnrouteWaypointRow {
    area_code: String,
    continent: String,
    country: String,
    datum_code: String,
    icao_code: String,
    waypoint_identifier: String,
    waypoint_latitude: f64,
    waypoint_longitude: f64,
    waypoint_name: String,
    waypoint_type: String,
    waypoint_usage: String,
    id: String,
}

fn area_code_for_icao(icao_code: &str) -> &'static str {
    match icao_code {
        "VH" | "VM" => "PAC",
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
            "SELECT icao_code, waypoint_identifier FROM {} WHERE (icao_code, waypoint_identifier) IN ({})",
            quote_sqlite_identifier(table_name),
            placeholders
        );
        let params = chunk
            .iter()
            .flat_map(|(icao_code, waypoint_identifier)| {
                [
                    SqlValue::Text(icao_code.clone()),
                    SqlValue::Text(waypoint_identifier.clone()),
                ]
            })
            .collect::<Vec<_>>();
        conn.query_each_native(&query, &params, |row| {
            let icao_code: String = row.get(0)?;
            let waypoint_identifier: String = row.get(1)?;
            pairs.insert((icao_code, waypoint_identifier));
            Ok(())
        })
        .map_err(sqlite_error)?;
    }

    Ok(pairs)
}

fn ensure_enroute_waypoints_index(conn: &RustSqliteConnection, table_name: &str) -> Result<()> {
    let index_name = format!("idx_{}_icao_identifier", table_name);
    let sql = format!(
        "CREATE INDEX IF NOT EXISTS {} ON {}(icao_code, waypoint_identifier)",
        quote_sqlite_identifier(&index_name),
        quote_sqlite_identifier(table_name)
    );
    conn.execute_statement_native(&sql, &[]).map_err(sqlite_error)?;
    Ok(())
}

fn bind_enroute_row_for_columns(
    stmt: &mut Statement<'_>,
    row: &EnrouteWaypointRow,
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
            "waypoint_identifier" => {
                stmt.raw_bind_parameter(parameter_index, row.waypoint_identifier.as_str())?
            }
            "waypoint_latitude" => stmt.raw_bind_parameter(parameter_index, row.waypoint_latitude)?,
            "waypoint_longitude" => stmt.raw_bind_parameter(parameter_index, row.waypoint_longitude)?,
            "waypoint_name" => stmt.raw_bind_parameter(parameter_index, row.waypoint_name.as_str())?,
            "waypoint_type" => stmt.raw_bind_parameter(parameter_index, row.waypoint_type.as_str())?,
            "waypoint_usage" => stmt.raw_bind_parameter(parameter_index, row.waypoint_usage.as_str())?,
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
    rows: &[EnrouteWaypointRow],
    magnetic_variations: Option<&[f64]>,
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    if magnetic_variations.is_some_and(|values| values.len() != rows.len()) {
        return Err(anyhow!("rows and declinations length mismatch"));
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
                    bind_enroute_row_for_columns(
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

pub(crate) fn process_enroute_waypoints_to_db(
    input_path: &str,
    conn: &RustSqliteConnection,
) -> Result<usize> {
    let parsed_rows = parse_enroute_waypoints_file(input_path)
        .map_err(|err| anyhow!("parse_enroute_waypoints_file failed: {}", err))?;

    conn.execute_statement_native(
            "CREATE TABLE IF NOT EXISTS tbl_enroute_waypoints (area_code TEXT, icao_code TEXT, waypoint_identifier TEXT, waypoint_name TEXT, waypoint_type TEXT, waypoint_usage TEXT, waypoint_latitude REAL, waypoint_longitude REAL, id TEXT)",
            &[],
        )
        .map_err(sqlite_error)?;
    ensure_enroute_waypoints_index(conn, ENROUTE_WAYPOINTS_TABLE)?;

    let unique_pairs = parsed_rows
        .iter()
        .map(|(icao_code, waypoint_identifier, _, _, _)| {
            (icao_code.clone(), waypoint_identifier.clone())
        })
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let existing_pairs = fetch_existing_pairs_for_keys(conn, ENROUTE_WAYPOINTS_TABLE, &unique_pairs, 500)
        .map_err(|err| anyhow!("fetch_existing_pairs_for_keys failed: {}", err))?;

    let mut coordinates = Vec::new();
    let mut rows: Vec<(String, String, String, f64, f64)> = Vec::new();
    for (icao_code, waypoint_identifier, waypoint_type, latitude, longitude) in parsed_rows {
        if existing_pairs.contains(&(icao_code.clone(), waypoint_identifier.clone())) {
            continue;
        }
        coordinates.push((latitude, longitude));
        rows.push((
            icao_code,
            waypoint_identifier,
            waypoint_type,
            latitude,
            longitude,
        ));
    }

    if rows.is_empty() {
        return Ok(0);
    }

    let columns = conn.get_table_columns_native(ENROUTE_WAYPOINTS_TABLE)?;
    let magnetic_variations = if columns.iter().any(|column| column == "magnetic_variation") {
        Some(
            batch_get_magnetic_variations_internal(&coordinates)
                .map_err(|err| anyhow!("batch_get_magnetic_variations_internal failed: {}", err))?,
        )
    } else {
        None
    };

    let insert_rows_payload: Vec<EnrouteWaypointRow> = rows
        .into_iter()
        .map(
            |(icao_code, waypoint_identifier, waypoint_type, latitude, longitude)| EnrouteWaypointRow {
                id: format!("{}{}", icao_code, waypoint_identifier),
                area_code: area_code_for_icao(&icao_code).to_string(),
                continent: "ASIA".to_string(),
                country: "CHINA".to_string(),
                datum_code: "WGE".to_string(),
                icao_code,
                waypoint_identifier: waypoint_identifier.clone(),
                waypoint_latitude: latitude,
                waypoint_longitude: longitude,
                waypoint_name: waypoint_identifier,
                waypoint_type,
                waypoint_usage: "RB".to_string(),
            },
        )
        .collect();

    insert_rows(
        conn,
        ENROUTE_WAYPOINTS_TABLE,
        &columns,
        &insert_rows_payload,
        magnetic_variations.as_deref(),
    )
        .map_err(|err| anyhow!("insert_rows failed: {}", err))?;
    Ok(insert_rows_payload.len())
}

fn sqlite_error(err: rusqlite::Error) -> anyhow::Error {
    anyhow!(err.to_string())
}
