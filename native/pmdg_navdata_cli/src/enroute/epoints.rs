use crate::core::db::RustSqliteConnection;
use crate::core::magnetic::batch_get_magnetic_variations_internal;
use crate::core::parsers::parse_enroute_waypoints_file;
use anyhow::{anyhow, Result};
use rusqlite::types::Value as SqlValue;
use std::collections::HashSet;

const SQLITE_MAX_VARIABLE_NUMBER: usize = 999;

#[derive(Clone)]
struct EnrouteWaypointRow {
    area_code: String,
    continent: String,
    country: String,
    datum_code: String,
    icao_code: String,
    magnetic_variation: f64,
    waypoint_identifier: String,
    waypoint_latitude: f64,
    waypoint_longitude: f64,
    waypoint_name: String,
    waypoint_type: String,
    waypoint_usage: String,
}

fn fetch_existing_pairs_for_keys(
    conn: &RustSqliteConnection,
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
            "SELECT icao_code, waypoint_identifier FROM tbl_ea_enroute_waypoints WHERE (icao_code, waypoint_identifier) IN ({})",
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

fn bind_enroute_row(
    stmt: &mut rusqlite::Statement<'_>,
    row: &EnrouteWaypointRow,
) -> rusqlite::Result<()> {
    stmt.raw_bind_parameter(1, row.area_code.as_str())?;
    stmt.raw_bind_parameter(2, row.continent.as_str())?;
    stmt.raw_bind_parameter(3, row.country.as_str())?;
    stmt.raw_bind_parameter(4, row.datum_code.as_str())?;
    stmt.raw_bind_parameter(5, row.icao_code.as_str())?;
    stmt.raw_bind_parameter(6, row.magnetic_variation)?;
    stmt.raw_bind_parameter(7, row.waypoint_identifier.as_str())?;
    stmt.raw_bind_parameter(8, row.waypoint_latitude)?;
    stmt.raw_bind_parameter(9, row.waypoint_longitude)?;
    stmt.raw_bind_parameter(10, row.waypoint_name.as_str())?;
    stmt.raw_bind_parameter(11, row.waypoint_type.as_str())?;
    stmt.raw_bind_parameter(12, row.waypoint_usage.as_str())?;
    stmt.raw_execute()?;
    Ok(())
}

fn insert_rows(conn: &RustSqliteConnection, rows: &[EnrouteWaypointRow]) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }

    let sql = "INSERT INTO tbl_ea_enroute_waypoints (area_code, continent, country, datum_code, icao_code, magnetic_variation, waypoint_identifier, waypoint_latitude, waypoint_longitude, waypoint_name, waypoint_type, waypoint_usage) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    conn.with_connection_native(|raw_conn| {
        let batch = 500;
        for start in (0..rows.len()).step_by(batch) {
            let end = (start + batch).min(rows.len());
            let tx = raw_conn.unchecked_transaction()?;
            {
                let mut stmt = tx.prepare(sql)?;
                for row in rows.iter().take(end).skip(start) {
                    bind_enroute_row(&mut stmt, row)?;
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
            "CREATE TABLE IF NOT EXISTS tbl_ea_enroute_waypoints (area_code TEXT, continent TEXT, country TEXT, datum_code TEXT, icao_code TEXT, magnetic_variation REAL, waypoint_identifier TEXT, waypoint_latitude REAL, waypoint_longitude REAL, waypoint_name TEXT, waypoint_type TEXT, waypoint_usage TEXT)",
            &[],
        )
        .map_err(sqlite_error)?;
    conn.execute_statement_native(
            "CREATE INDEX IF NOT EXISTS idx_waypoints_icao_identifier ON tbl_ea_enroute_waypoints(icao_code, waypoint_identifier)",
            &[],
        )
        .map_err(sqlite_error)?;

    let unique_pairs = parsed_rows
        .iter()
        .map(|(icao_code, waypoint_identifier, _, _, _)| {
            (icao_code.clone(), waypoint_identifier.clone())
        })
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let existing_pairs = fetch_existing_pairs_for_keys(conn, &unique_pairs, 500)
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

    let declinations = batch_get_magnetic_variations_internal(&coordinates)
        .map_err(|err| anyhow!("batch_get_magnetic_variations_internal failed: {}", err))?;

    let insert_rows_payload: Vec<EnrouteWaypointRow> = rows
        .into_iter()
        .zip(declinations)
        .map(
            |(
                (icao_code, waypoint_identifier, waypoint_type, latitude, longitude),
                magnetic_variation,
            )| EnrouteWaypointRow {
                area_code: "EEU".to_string(),
                continent: "ASIA".to_string(),
                country: "CHINA".to_string(),
                datum_code: "WGE".to_string(),
                icao_code,
                magnetic_variation,
                waypoint_identifier: waypoint_identifier.clone(),
                waypoint_latitude: latitude,
                waypoint_longitude: longitude,
                waypoint_name: waypoint_identifier,
                waypoint_type,
                waypoint_usage: "RB".to_string(),
            },
        )
        .collect();

    insert_rows(conn, &insert_rows_payload)
        .map_err(|err| anyhow!("insert_rows failed: {}", err))?;
    Ok(insert_rows_payload.len())
}

fn sqlite_error(err: rusqlite::Error) -> anyhow::Error {
    anyhow!(err.to_string())
}
