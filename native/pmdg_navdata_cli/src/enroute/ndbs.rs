use crate::core::db::RustSqliteConnection;
use crate::core::magnetic::batch_get_magnetic_variations_internal;
use crate::core::parsers::parse_ndb_nav_file;
use anyhow::{anyhow, Result};
use rusqlite::types::Value as SqlValue;
use std::collections::HashSet;

const SQLITE_MAX_VARIABLE_NUMBER: usize = 999;

#[derive(Clone)]
struct NdbInsertRow {
    area_code: String,
    continent: String,
    country: String,
    datum_code: String,
    icao_code: String,
    magnetic_variation: f64,
    navaid_class: String,
    navaid_frequency: f64,
    navaid_identifier: String,
    navaid_latitude: f64,
    navaid_longitude: f64,
    navaid_name: String,
    range: f64,
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
            "SELECT navaid_identifier, icao_code FROM tbl_db_enroute_ndbnavaids WHERE (navaid_identifier, icao_code) IN ({})",
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

fn bind_ndb_row(stmt: &mut rusqlite::Statement<'_>, row: &NdbInsertRow) -> rusqlite::Result<()> {
    stmt.raw_bind_parameter(1, row.area_code.as_str())?;
    stmt.raw_bind_parameter(2, row.continent.as_str())?;
    stmt.raw_bind_parameter(3, row.country.as_str())?;
    stmt.raw_bind_parameter(4, row.datum_code.as_str())?;
    stmt.raw_bind_parameter(5, row.icao_code.as_str())?;
    stmt.raw_bind_parameter(6, row.magnetic_variation)?;
    stmt.raw_bind_parameter(7, row.navaid_class.as_str())?;
    stmt.raw_bind_parameter(8, row.navaid_frequency)?;
    stmt.raw_bind_parameter(9, row.navaid_identifier.as_str())?;
    stmt.raw_bind_parameter(10, row.navaid_latitude)?;
    stmt.raw_bind_parameter(11, row.navaid_longitude)?;
    stmt.raw_bind_parameter(12, row.navaid_name.as_str())?;
    stmt.raw_bind_parameter(13, row.range)?;
    stmt.raw_execute()?;
    Ok(())
}

fn insert_rows(conn: &RustSqliteConnection, rows: &[NdbInsertRow]) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }

    let sql = "INSERT INTO tbl_db_enroute_ndbnavaids (area_code, continent, country, datum_code, icao_code, magnetic_variation, navaid_class, navaid_frequency, navaid_identifier, navaid_latitude, navaid_longitude, navaid_name, range) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    conn.with_connection_native(|raw_conn| {
        let batch = 500;
        for start in (0..rows.len()).step_by(batch) {
            let end = (start + batch).min(rows.len());
            let tx = raw_conn.unchecked_transaction()?;
            {
                let mut stmt = tx.prepare(sql)?;
                for row in rows.iter().take(end).skip(start) {
                    bind_ndb_row(&mut stmt, row)?;
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
            "\n        CREATE TABLE IF NOT EXISTS tbl_db_enroute_ndbnavaids (\n            area_code TEXT DEFAULT 'EEU',\n            continent TEXT DEFAULT 'ASIA',\n            country TEXT DEFAULT 'CHINA',\n            datum_code TEXT DEFAULT 'WGE',\n            icao_code TEXT,\n            magnetic_variation REAL,\n            navaid_class TEXT DEFAULT 'H W',\n            navaid_frequency REAL,\n            navaid_identifier TEXT,\n            navaid_latitude REAL,\n            navaid_longitude REAL,\n            navaid_name TEXT,\n            range REAL\n        )\n        ",
            &[],
        )
        .map_err(sqlite_error)?;

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
    let existing_pairs = fetch_existing_pairs_for_keys(conn, &unique_pairs, 500)
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

    let declinations = batch_get_magnetic_variations_internal(&coordinates)
        .map_err(|err| anyhow!("batch_get_magnetic_variations_internal failed: {}", err))?;

    let rows: Vec<NdbInsertRow> = pending_rows
        .into_iter()
        .zip(declinations)
        .map(
            |(
                (
                    icao_code,
                    navaid_identifier,
                    navaid_name,
                    navaid_frequency,
                    navaid_latitude,
                    navaid_longitude,
                    ndb_range,
                ),
                magnetic_variation,
            )| NdbInsertRow {
                area_code: "EEU".to_string(),
                continent: "ASIA".to_string(),
                country: "CHINA".to_string(),
                datum_code: "WGE".to_string(),
                icao_code,
                magnetic_variation,
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

    insert_rows(conn, &rows).map_err(|err| anyhow!("insert_rows failed: {}", err))?;
    Ok(rows.len())
}

fn sqlite_error(err: rusqlite::Error) -> anyhow::Error {
    anyhow!(err.to_string())
}
