use crate::core::db::{
    get_shared_connection, join_quoted_sqlite_identifiers, open_sqlite_connection,
    quote_sqlite_identifier, RustSqliteConnection,
};
use crate::core::parsers::parse_terminal_waypoints_file;
use crate::core::magnetic::batch_get_magnetic_variations_internal;
use anyhow::{anyhow, Result};
use rusqlite::types::Null;
use rusqlite::types::Value as SqlValue;
use rusqlite::Statement;
use std::collections::{HashMap, HashSet};

const SQLITE_MAX_VARIABLE_NUMBER: usize = 999;
const PAIR_QUERY_PARAMETER_COUNT: usize = 2;

struct TerminalWaypointRecord {
    area_code: String,
    continent: String,
    country: String,
    datum_code: String,
    icao_code: String,
    region_code: String,
    waypoint_identifier: String,
    waypoint_latitude: f64,
    waypoint_longitude: f64,
    waypoint_name: String,
    waypoint_type: String,
    id: String,
}

impl TerminalWaypointRecord {
    fn from_parsed(
        region_code: String,
        icao_code: String,
        waypoint_identifier: String,
        waypoint_name: String,
        waypoint_type: String,
        lat: f64,
        lon: f64,
    ) -> Self {
        let id = format!("{}{}{}", region_code, icao_code, waypoint_identifier);
        Self {
            area_code: "EEU".to_string(),
            continent: "ASIA".to_string(),
            country: "CHINA".to_string(),
            datum_code: "WGE".to_string(),
            icao_code,
            region_code,
            waypoint_identifier,
            waypoint_latitude: lat,
            waypoint_longitude: lon,
            waypoint_name,
            waypoint_type,
            id,
        }
    }
}

fn pair_query_batch_size(batch_size: usize) -> usize {
    let max_pairs_per_query = SQLITE_MAX_VARIABLE_NUMBER / PAIR_QUERY_PARAMETER_COUNT;
    batch_size.max(1).min(max_pairs_per_query.max(1))
}

fn fetch_existing_pairs(
    conn: &RustSqliteConnection,
    table_name: &str,
    unique_pairs: &[(String, String)],
    batch_size: usize,
) -> Result<HashMap<String, HashSet<String>>> {
    let mut existing_pairs: HashMap<String, HashSet<String>> = HashMap::new();
    if unique_pairs.is_empty() {
        return Ok(existing_pairs);
    }

    let actual_batch_size = pair_query_batch_size(batch_size);
    for batch in unique_pairs.chunks(actual_batch_size) {
        let placeholders = vec!["(?,?)"; batch.len()].join(",");
        let query = format!(
            "SELECT region_code, waypoint_identifier FROM {} WHERE (region_code, waypoint_identifier) IN ({})",
            quote_sqlite_identifier(table_name),
            placeholders
        );
        let params = batch
            .iter()
            .flat_map(|(region_code, waypoint_identifier)| {
                [
                    SqlValue::Text(region_code.clone()),
                    SqlValue::Text(waypoint_identifier.clone()),
                ]
            })
            .collect::<Vec<_>>();
        conn.query_each_native(&query, &params, |row| {
            let region_code: String = row.get(0)?;
            let waypoint_identifier: String = row.get(1)?;
            existing_pairs
                .entry(region_code)
                .or_default()
                .insert(waypoint_identifier);
            Ok(())
        })?;
    }

    Ok(existing_pairs)
}

fn ensure_terminal_waypoints_index_native(
    conn: &RustSqliteConnection,
    table_name: &str,
) -> Result<()> {
    let index_name = format!("idx_{}_region_identifier", table_name);
    let sql = format!(
        "CREATE INDEX IF NOT EXISTS {} ON {}(region_code, waypoint_identifier)",
        quote_sqlite_identifier(&index_name),
        quote_sqlite_identifier(table_name)
    );
    conn.execute_statement_native(&sql, &[])?;
    Ok(())
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

fn bind_row_for_columns(
    stmt: &mut Statement<'_>,
    record: &TerminalWaypointRecord,
    magnetic_variation: Option<f64>,
    columns: &[String],
) -> rusqlite::Result<()> {
    for (index, column) in columns.iter().enumerate() {
        let parameter_index = index + 1;
        match column.as_str() {
            "area_code" => stmt.raw_bind_parameter(parameter_index, record.area_code.as_str())?,
            "continent" => stmt.raw_bind_parameter(parameter_index, record.continent.as_str())?,
            "country" => stmt.raw_bind_parameter(parameter_index, record.country.as_str())?,
            "datum_code" => stmt.raw_bind_parameter(parameter_index, record.datum_code.as_str())?,
            "icao_code" => stmt.raw_bind_parameter(parameter_index, record.icao_code.as_str())?,
            "magnetic_variation" => {
                if let Some(value) = magnetic_variation {
                    stmt.raw_bind_parameter(parameter_index, value)?
                } else {
                    stmt.raw_bind_parameter(parameter_index, Null)?
                }
            }
            "region_code" => {
                stmt.raw_bind_parameter(parameter_index, record.region_code.as_str())?
            }
            "waypoint_identifier" => {
                stmt.raw_bind_parameter(parameter_index, record.waypoint_identifier.as_str())?
            }
            "waypoint_latitude" => {
                stmt.raw_bind_parameter(parameter_index, record.waypoint_latitude)?
            }
            "waypoint_longitude" => {
                stmt.raw_bind_parameter(parameter_index, record.waypoint_longitude)?
            }
            "waypoint_name" => {
                stmt.raw_bind_parameter(parameter_index, record.waypoint_name.as_str())?
            }
            "waypoint_type" => {
                stmt.raw_bind_parameter(parameter_index, record.waypoint_type.as_str())?
            }
            "id" => stmt.raw_bind_parameter(parameter_index, record.id.as_str())?,
            _ => stmt.raw_bind_parameter(parameter_index, Null)?,
        }
    }

    stmt.raw_execute()?;
    Ok(())
}

fn insert_projected_rows(
    conn: &RustSqliteConnection,
    table_name: &str,
    columns: &[String],
    records: &[TerminalWaypointRecord],
    magnetic_variations: Option<&[f64]>,
    batch_size: usize,
) -> Result<()> {
    if records.is_empty() {
        return Ok(());
    }
    if magnetic_variations.is_some_and(|values| records.len() != values.len()) {
        return Err(anyhow!("records and declinations length mismatch"));
    }

    let query = build_insert_sql(table_name, columns);
    let actual_batch_size = batch_size.max(1);

    conn.with_connection_native(|raw_conn| {
        for start in (0..records.len()).step_by(actual_batch_size) {
            let end = (start + actual_batch_size).min(records.len());
            let tx = raw_conn.unchecked_transaction()?;
            {
                let mut stmt = tx.prepare(query.as_str())?;
                for idx in start..end {
                    bind_row_for_columns(
                        &mut stmt,
                        &records[idx],
                        magnetic_variations.map(|values| values[idx]),
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

fn convert_terminal_waypoints_file_to_db(
    file_path: &str,
    conn: &RustSqliteConnection,
    table_name: &str,
    query_batch_size: usize,
    insert_batch_size: usize,
) -> Result<(usize, usize)> {
    ensure_terminal_waypoints_index_native(conn, table_name)?;

    let parsed = parse_terminal_waypoints_file(file_path)
        .map_err(|err| anyhow!("parse_terminal_waypoints_file failed: {}", err))?;
    let parsed_count = parsed.len();
    let records: Vec<TerminalWaypointRecord> = parsed
        .into_iter()
        .map(
            |(
                region_code,
                icao_code,
                waypoint_identifier,
                waypoint_name,
                waypoint_type,
                lat,
                lon,
            )| {
                TerminalWaypointRecord::from_parsed(
                    region_code,
                    icao_code,
                    waypoint_identifier,
                    waypoint_name,
                    waypoint_type,
                    lat,
                    lon,
                )
            },
        )
        .collect();

    if records.is_empty() {
        return Ok((0, 0));
    }

    let unique_pairs: Vec<(String, String)> = records
        .iter()
        .map(|record| {
            (
                record.region_code.clone(),
                record.waypoint_identifier.clone(),
            )
        })
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    let existing_pairs = fetch_existing_pairs(conn, table_name, &unique_pairs, query_batch_size)?;

    let new_records: Vec<TerminalWaypointRecord> = records
        .into_iter()
        .filter(|record| {
            existing_pairs
                .get(record.region_code.as_str())
                .is_none_or(|identifiers| {
                    !identifiers.contains(record.waypoint_identifier.as_str())
                })
        })
        .collect();

    let new_count = new_records.len();
    if new_records.is_empty() {
        return Ok((parsed_count, 0));
    }

    let columns = conn.get_table_columns_native(table_name)?;
    let magnetic_variations = if columns.iter().any(|column| column == "magnetic_variation") {
        let coords = new_records
            .iter()
            .map(|record| (record.waypoint_latitude, record.waypoint_longitude))
            .collect::<Vec<_>>();
        Some(batch_get_magnetic_variations_internal(&coords)?)
    } else {
        None
    };
    insert_projected_rows(
        conn,
        table_name,
        &columns,
        &new_records,
        magnetic_variations.as_deref(),
        insert_batch_size,
    )?;

    Ok((parsed_count, new_count))
}

pub(crate) fn process_terminal_waypoints_file_to_db(
    file_path: &str,
    db_path: &str,
    table_name: &str,
    timeout: u32,
    query_batch_size: usize,
    insert_batch_size: usize,
) -> Result<(usize, usize)> {
    let shared_conn = get_shared_connection(db_path)?;
    let owns_connection = shared_conn.is_none();
    let conn = match shared_conn {
        Some(conn) => conn,
        None => open_sqlite_connection(db_path, timeout)?,
    };

    let result = convert_terminal_waypoints_file_to_db(
        file_path,
        &conn,
        table_name,
        query_batch_size,
        insert_batch_size,
    );

    if owns_connection {
        conn.close_native();
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    #[test]
    fn clamps_pair_query_batch_size_to_sqlite_limit() {
        assert_eq!(
            pair_query_batch_size(500),
            SQLITE_MAX_VARIABLE_NUMBER / PAIR_QUERY_PARAMETER_COUNT
        );
        assert_eq!(pair_query_batch_size(0), 1);
        assert_eq!(pair_query_batch_size(200), 200);
    }

    #[test]
    fn binds_terminal_waypoint_row_by_columns() {
        let row = TerminalWaypointRecord::from_parsed(
            "TERM".to_string(),
            "ZS".to_string(),
            "FIX01".to_string(),
            "FIX01".to_string(),
            "A".to_string(),
            31.1,
            121.2,
        );
        let columns = vec![
            "region_code".to_string(),
            "waypoint_identifier".to_string(),
            "waypoint_latitude".to_string(),
            "waypoint_longitude".to_string(),
            "magnetic_variation".to_string(),
        ];
        let query = build_insert_sql("test_terminal_waypoints", &columns);

        let conn = Connection::open_in_memory().unwrap();
        conn.execute(
            "CREATE TABLE test_terminal_waypoints (region_code TEXT, waypoint_identifier TEXT, waypoint_latitude REAL, waypoint_longitude REAL, magnetic_variation REAL)",
            [],
        )
        .unwrap();

        let tx = conn.unchecked_transaction().unwrap();
        {
            let mut stmt = tx.prepare(&query).unwrap();
            bind_row_for_columns(&mut stmt, &row, Some(3.5), &columns).unwrap();
        }
        tx.commit().unwrap();

        let inserted = conn
            .query_row(
                "SELECT region_code, waypoint_identifier, waypoint_latitude, waypoint_longitude, magnetic_variation FROM test_terminal_waypoints",
                [],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, f64>(2)?,
                        row.get::<_, f64>(3)?,
                        row.get::<_, f64>(4)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(inserted.0, "TERM");
        assert_eq!(inserted.1, "FIX01");
        assert!((inserted.2 - 31.1).abs() < f64::EPSILON);
        assert!((inserted.3 - 121.2).abs() < f64::EPSILON);
        assert!((inserted.4 - 3.5).abs() < f64::EPSILON);
    }

    #[test]
    fn binds_terminal_waypoint_id_for_new_schema() {
        let row = TerminalWaypointRecord::from_parsed(
            "01OH".to_string(),
            "K5".to_string(),
            "WADON".to_string(),
            "WADON".to_string(),
            "WMZ".to_string(),
            39.49556389,
            -84.30007778,
        );
        let columns = vec![
            "region_code".to_string(),
            "icao_code".to_string(),
            "waypoint_identifier".to_string(),
            "id".to_string(),
        ];
        let query = build_insert_sql("test_terminal_waypoints_new", &columns);

        let mut conn = Connection::open_in_memory().unwrap();
        conn.execute(
            "CREATE TABLE test_terminal_waypoints_new (region_code TEXT, icao_code TEXT, waypoint_identifier TEXT, id TEXT)",
            [],
        )
        .unwrap();

        let tx = conn.unchecked_transaction().unwrap();
        {
            let mut stmt = tx.prepare(&query).unwrap();
            bind_row_for_columns(&mut stmt, &row, None, &columns).unwrap();
        }
        tx.commit().unwrap();

        let inserted = conn
            .query_row(
                "SELECT region_code, icao_code, waypoint_identifier, id FROM test_terminal_waypoints_new",
                [],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(inserted.0, "01OH");
        assert_eq!(inserted.1, "K5");
        assert_eq!(inserted.2, "WADON");
        assert_eq!(inserted.3, "01OHK5WADON");
    }
}
