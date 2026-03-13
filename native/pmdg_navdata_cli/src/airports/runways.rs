use crate::core::db::RustSqliteConnection;
use anyhow::{anyhow, Result};
use csv::{ReaderBuilder, StringRecord, Trim};
use encoding_rs::Encoding;
use rusqlite::types::Value as SqlValue;
use std::collections::{HashMap, HashSet};
use std::fs;

const FEET_PER_METER: f64 = 3.28084;
const AIRPORT_SKIP_LIST: &[&str] = &[
    "ZBAA", "ZBAD", "ZBDS", "ZBER", "ZBDT", "ZBHH", "ZBLA", "ZBMZ", "ZBOW", "ZBSJ", "ZBTJ", "ZBYC",
    "ZBYN", "ZGDY", "ZGGG", "ZGHA", "ZGKL", "ZGNN", "ZGOW", "ZGSZ", "ZGZJ", "ZHCC", "ZHEC", "ZHES",
    "ZHHH", "ZHXF", "ZHYC", "ZJHK", "ZJQH", "ZJSY", "ZL02", "ZL03", "ZLDH", "ZLIC", "ZLLL", "ZLXN",
    "ZLXY", "ZPJH", "ZPLJ", "ZPMS", "ZPPP", "ZSAM", "ZSCG", "ZSCN", "ZSFZ", "ZSHC", "ZSJN", "ZSLG",
    "ZSLY", "ZSNB", "ZSNJ", "ZSNT", "ZSOF", "ZSPD", "ZSQD", "ZSQZ", "ZSSH", "ZSSS", "ZSTX", "ZSWA",
    "ZSWH", "ZSWX", "ZSWZ", "ZSXZ", "ZSYA", "ZSYN", "ZSYT", "ZSYW", "ZSZS", "ZUCK", "ZUGY", "ZULS",
    "ZUTF", "ZUUU", "ZUXC", "ZW01", "ZW02", "ZWSH", "ZWTN", "ZWWW", "ZWYN", "ZYCC", "ZYHB", "ZYJM",
    "ZYMD", "ZYQQ", "ZYTL", "ZYTX", "ZYYJ",
];
const SUPPLEMENTARY_ICAOS: &[&str] = &["ZLYX", "ZUNZ", "ZURK", "ZLYS", "ZPLJ"];

#[derive(Clone)]
struct FirstCsvRow {
    rwy_id: String,
    txt_desig: String,
    val_true_brg: Option<f64>,
    val_elev: Option<f64>,
    val_thr_displace: Option<f64>,
}

#[derive(Clone)]
struct SecondCsvRow {
    code_airport: String,
    val_len: Option<f64>,
    val_wid: Option<f64>,
    num_surface: i64,
}

#[derive(Clone)]
struct RunwayInsertRow {
    airport_identifier: String,
    area_code: String,
    displaced_threshold_distance: i64,
    icao_code: String,
    landing_threshold_elevation: i64,
    llz_identifier: Option<String>,
    llz_mls_gls_category: Option<i64>,
    runway_gradient: f64,
    runway_identifier: String,
    runway_latitude: f64,
    runway_length: i64,
    runway_longitude: f64,
    runway_magnetic_bearing: f64,
    runway_true_bearing: f64,
    runway_width: i64,
    surface_code: String,
    threshold_crossing_height: i64,
    id: String,
}

fn area_code_for_icao(icao_code: &str) -> &'static str {
    match icao_code {
        "VH" | "VM" => "PAC",
        _ => "EEU",
    }
}

fn build_insert_sql() -> &'static str {
    "INSERT OR IGNORE INTO tbl_runways (area_code, icao_code, airport_identifier, runway_identifier, runway_latitude, runway_longitude, runway_gradient, runway_magnetic_bearing, runway_true_bearing, landing_threshold_elevation, displaced_threshold_distance, threshold_crossing_height, runway_length, runway_width, llz_identifier, llz_mls_gls_category, surface_code, id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
}

fn python_round(value: f64) -> i64 {
    let truncated = value.trunc();
    let fractional = value - truncated;
    let abs_fractional = fractional.abs();
    if (abs_fractional - 0.5).abs() > 1e-12 {
        return value.round() as i64;
    }

    let lower = value.floor();
    let upper = value.ceil();
    let lower_int = lower as i64;
    let upper_int = upper as i64;
    if lower_int % 2 == 0 {
        lower_int
    } else if upper_int % 2 == 0 {
        upper_int
    } else {
        value.round() as i64
    }
}

fn decode_latin1_file(file_path: &str) -> Result<String> {
    let bytes =
        fs::read(file_path).map_err(|err| anyhow!("failed to read {}: {}", file_path, err))?;
    Ok(bytes.into_iter().map(char::from).collect())
}

fn decode_gb18030_file(file_path: &str) -> Result<String> {
    let bytes =
        fs::read(file_path).map_err(|err| anyhow!("failed to read {}: {}", file_path, err))?;
    let encoding = Encoding::for_label(b"gb18030")
        .ok_or_else(|| anyhow!("gb18030 encoding is unavailable"))?;
    let (text, _, _) = encoding.decode(&bytes);
    Ok(text.into_owned())
}

fn required_index(headers: &StringRecord, column: &str) -> Result<usize> {
    headers
        .iter()
        .position(|value| value == column)
        .ok_or_else(|| anyhow!("required runway CSV column missing: {}", column))
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

fn optional_f64(record: &StringRecord, index: Option<usize>) -> Option<f64> {
    optional_string(record, index).and_then(|value| value.parse::<f64>().ok())
}

fn parse_first_csv(file_path: &str) -> Result<Vec<FirstCsvRow>> {
    let content = decode_latin1_file(file_path)?;
    let mut reader = ReaderBuilder::new()
        .trim(Trim::None)
        .flexible(true)
        .from_reader(content.as_bytes());
    let headers = reader
        .headers()
        .map_err(|err| anyhow!("failed to read runway direction CSV headers: {}", err))?
        .clone();

    let rwy_id_idx = required_index(&headers, "RWY_ID")?;
    let txt_desig_idx = required_index(&headers, "TXT_DESIG")?;
    let val_true_brg_idx = optional_index(&headers, "VAL_TRUE_BRG");
    let val_elev_idx = optional_index(&headers, "VAL_ELEV");
    let val_thr_displace_idx = optional_index(&headers, "VAL_THR_DISPLACE");

    let mut rows = Vec::new();
    for record in reader.records() {
        let record =
            record.map_err(|err| anyhow!("failed to parse runway direction CSV row: {}", err))?;
        let Some(rwy_id) = record
            .get(rwy_id_idx)
            .map(str::trim)
            .filter(|value| !value.is_empty())
        else {
            continue;
        };
        let txt_desig = record
            .get(txt_desig_idx)
            .unwrap_or_default()
            .trim()
            .to_string();
        rows.push(FirstCsvRow {
            rwy_id: rwy_id.to_string(),
            txt_desig,
            val_true_brg: optional_f64(&record, val_true_brg_idx),
            val_elev: optional_f64(&record, val_elev_idx),
            val_thr_displace: optional_f64(&record, val_thr_displace_idx),
        });
    }
    Ok(rows)
}

fn parse_second_csv(file_path: &str) -> Result<HashMap<String, SecondCsvRow>> {
    let content = decode_latin1_file(file_path)?;
    let mut reader = ReaderBuilder::new()
        .trim(Trim::None)
        .flexible(true)
        .from_reader(content.as_bytes());
    let headers = reader
        .headers()
        .map_err(|err| anyhow!("failed to read runway CSV headers: {}", err))?
        .clone();

    let rwy_id_idx = required_index(&headers, "RWY_ID")?;
    let code_airport_idx = required_index(&headers, "CODE_AIRPORT")?;
    let val_len_idx = optional_index(&headers, "VAL_LEN");
    let val_wid_idx = optional_index(&headers, "VAL_WID");
    let num_surface_idx = optional_index(&headers, "NUM_SURFACE");

    let mut rows = HashMap::new();
    for record in reader.records() {
        let record = record.map_err(|err| anyhow!("failed to parse runway CSV row: {}", err))?;
        let Some(rwy_id) = record
            .get(rwy_id_idx)
            .map(str::trim)
            .filter(|value| !value.is_empty())
        else {
            continue;
        };
        let code_airport = record
            .get(code_airport_idx)
            .unwrap_or_default()
            .trim()
            .to_string();
        rows.insert(
            rwy_id.to_string(),
            SecondCsvRow {
                code_airport,
                val_len: optional_f64(&record, val_len_idx),
                val_wid: optional_f64(&record, val_wid_idx),
                num_surface: optional_f64(&record, num_surface_idx)
                    .map(|value| value as i64)
                    .unwrap_or(103),
            },
        );
    }
    Ok(rows)
}

fn parse_magvar_csv(file_path: &str) -> Result<HashMap<String, f64>> {
    let content = decode_gb18030_file(file_path)?;
    let mut reader = ReaderBuilder::new()
        .trim(Trim::All)
        .flexible(true)
        .from_reader(content.as_bytes());
    let headers = reader
        .headers()
        .map_err(|err| anyhow!("failed to read airport CSV headers: {}", err))?
        .clone();

    let code_id_idx = required_index(&headers, "CODE_ID")?;
    let val_mag_var_idx = optional_index(&headers, "VAL_MAG_VAR");
    let mut out = HashMap::new();
    for record in reader.records() {
        let record = record.map_err(|err| anyhow!("failed to parse airport CSV row: {}", err))?;
        let Some(code_id) = record
            .get(code_id_idx)
            .map(str::trim)
            .filter(|value| !value.is_empty())
        else {
            continue;
        };
        if out.contains_key(code_id) {
            continue;
        }
        if let Some(mag_var) = optional_f64(&record, val_mag_var_idx) {
            out.insert(code_id.to_string(), mag_var);
        }
    }
    Ok(out)
}

fn load_airport_data(nd_db_path: &str) -> Result<HashMap<(String, String), (f64, f64)>> {
    let conn = RustSqliteConnection::open_native(nd_db_path, 30).map_err(sqlite_error)?;
    let result = (|| {
        let mut out = HashMap::new();
        conn.query_each_native(
            "SELECT ICAO AS airport_icao, ident AS runway_ident, lati AS Latitude, longi AS Longtitude FROM runway",
            &[],
            |row| {
                let icao: String = row.get(0)?;
                let runway_ident: String = row.get(1)?;
                let Some(latitude) = row.get::<_, Option<f64>>(2)? else {
                    return Ok(());
                };
                let Some(longitude) = row.get::<_, Option<f64>>(3)? else {
                    return Ok(());
                };
                if icao.is_empty() || runway_ident.is_empty() {
                    return Ok(());
                }
                out.insert((icao, runway_ident), (latitude, longitude));
                Ok(())
            },
        )
        .map_err(sqlite_error)?;
        Ok(out)
    })();
    conn.close_native();
    result
}

fn load_ils_map(nd_db_path: &str) -> Result<HashMap<(String, String), Option<String>>> {
    let conn = RustSqliteConnection::open_native(nd_db_path, 30).map_err(sqlite_error)?;
    let result = (|| {
        let mut out = HashMap::new();
        conn.query_each_native(
            "SELECT rowid, ICAO, runway, ident FROM ils ORDER BY rowid",
            &[],
            |row| {
                let icao: String = row.get(1)?;
                let runway: String = row.get(2)?;
                if icao.is_empty() || runway.is_empty() {
                    return Ok(());
                }
                let ident: Option<String> = row.get(3)?;
                out.entry((icao, runway)).or_insert(ident);
                Ok(())
            },
        )
        .map_err(sqlite_error)?;
        Ok(out)
    })();
    conn.close_native();
    result
}

fn get_llz_info(
    airport_icao: &str,
    runway_identifier: &str,
    ils_map: &HashMap<(String, String), Option<String>>,
) -> (Option<String>, Option<i64>) {
    let Some(ident) = ils_map.get(&(airport_icao.to_string(), runway_identifier.to_string()))
    else {
        return (None, None);
    };
    match ident {
        Some(value) if !value.is_empty() => (Some(value.clone()), Some(1)),
        _ => (None, None),
    }
}

fn fetch_existing_runways(conn: &RustSqliteConnection) -> Result<HashSet<(String, String)>> {
    let mut out = HashSet::new();
    conn.query_each_native(
        "SELECT airport_identifier, runway_identifier FROM tbl_runways",
        &[],
        |row| {
            let airport_identifier: String = row.get(0)?;
            let runway_identifier: String = row.get(1)?;
            out.insert((airport_identifier, runway_identifier));
            Ok(())
        },
    )
    .map_err(sqlite_error)?;
    Ok(out)
}

fn surface_code_from_num_surface(num_surface: i64) -> String {
    match num_surface {
        100 => "ASPH".to_string(),
        103 => "CONC".to_string(),
        19 => "UNPV".to_string(),
        5 => "GRVL".to_string(),
        _ => "UNKNOWN".to_string(),
    }
}

fn build_primary_rows(
    first_rows: &[FirstCsvRow],
    second_by_id: &HashMap<String, SecondCsvRow>,
    magvar_map: &HashMap<String, f64>,
    airport_data: &HashMap<(String, String), (f64, f64)>,
    existing_runways: &HashSet<(String, String)>,
    ils_map: &HashMap<(String, String), Option<String>>,
) -> (Vec<RunwayInsertRow>, Vec<String>) {
    let skip_airports: HashSet<&str> = AIRPORT_SKIP_LIST.iter().copied().collect();
    let mut rows = Vec::new();
    let mut missing = Vec::new();

    for row in first_rows {
        let runway_identifier = row.txt_desig.trim();
        let Some(runway_data) = second_by_id.get(&row.rwy_id) else {
            continue;
        };

        let airport_icao = if runway_data.code_airport.is_empty() {
            "UNKNOWN".to_string()
        } else {
            runway_data.code_airport.clone()
        };
        if skip_airports.contains(airport_icao.as_str()) {
            continue;
        }

        let Some(runway_length_m) = runway_data.val_len else {
            continue;
        };
        let Some(runway_width_m) = runway_data.val_wid else {
            continue;
        };
        let Some((runway_lat, runway_lon)) =
            airport_data.get(&(airport_icao.clone(), runway_identifier.to_string()))
        else {
            missing.push(format!(
                "Missing coordinates for {} RW{}",
                airport_icao, runway_identifier
            ));
            continue;
        };

        let formatted_runway_identifier = format!("RW{}", runway_identifier.trim());
        if existing_runways.contains(&(airport_icao.clone(), formatted_runway_identifier.clone())) {
            continue;
        }

        let Some(true_bearing) = row.val_true_brg else {
            continue;
        };
        let mag_var = magvar_map.get(&airport_icao).copied().unwrap_or(0.0);
        let mag_bearing = python_round(true_bearing - mag_var);
        let threshold_elev = row
            .val_elev
            .map(|value| python_round(value * FEET_PER_METER))
            .unwrap_or(0);
        let displaced_dist = row
            .val_thr_displace
            .map(|value| python_round(value * FEET_PER_METER))
            .unwrap_or(0);
        let runway_length = python_round(runway_length_m * FEET_PER_METER);
        let runway_width = python_round(runway_width_m * FEET_PER_METER);
        let (llz_identifier, llz_mls_gls_category) =
            get_llz_info(&airport_icao, runway_identifier, ils_map);
        let icao_code = if airport_icao.len() >= 2 {
            airport_icao[..2].to_string()
        } else {
            "UN".to_string()
        };

        rows.push(RunwayInsertRow {
            airport_identifier: airport_icao.clone(),
            area_code: area_code_for_icao(&icao_code).to_string(),
            displaced_threshold_distance: displaced_dist,
            icao_code: icao_code.clone(),
            landing_threshold_elevation: threshold_elev,
            llz_identifier,
            llz_mls_gls_category,
            runway_gradient: 0.0,
            runway_identifier: formatted_runway_identifier.clone(),
            runway_latitude: ((*runway_lat * 1e8).round()) / 1e8,
            runway_length,
            runway_longitude: ((*runway_lon * 1e8).round()) / 1e8,
            runway_magnetic_bearing: mag_bearing as f64,
            runway_true_bearing: true_bearing,
            runway_width,
            surface_code: surface_code_from_num_surface(runway_data.num_surface),
            threshold_crossing_height: 50,
            id: format!("{}{}{}", airport_icao, icao_code, formatted_runway_identifier),
        });
    }

    (rows, missing)
}

fn build_supplementary_rows(
    nd_db_path: &str,
    existing_runways: &HashSet<(String, String)>,
    ils_map: &HashMap<(String, String), Option<String>>,
) -> Result<Vec<RunwayInsertRow>> {
    let conn = RustSqliteConnection::open_native(nd_db_path, 30).map_err(sqlite_error)?;
    let placeholders = SUPPLEMENTARY_ICAOS
        .iter()
        .map(|_| "?")
        .collect::<Vec<_>>()
        .join(",");
    let sql = format!(
        "SELECT ICAO, ident, lati, longi, length, width, geo, mag, alt FROM runway WHERE ICAO IN ({})",
        placeholders
    );
    let params = SUPPLEMENTARY_ICAOS
        .iter()
        .map(|icao| SqlValue::Text((*icao).to_string()))
        .collect::<Vec<_>>();
    let result = (|| {
        let mut out = Vec::new();
        conn.query_each_native(&sql, &params, |row| {
            let icao: String = row.get(0)?;
            let ident: String = row.get(1)?;
            let Some(runway_latitude) = row.get::<_, Option<f64>>(2)? else {
                return Ok(());
            };
            let Some(runway_longitude) = row.get::<_, Option<f64>>(3)? else {
                return Ok(());
            };
            let Some(runway_length_m) = row.get::<_, Option<f64>>(4)? else {
                return Ok(());
            };
            let Some(runway_width_m) = row.get::<_, Option<f64>>(5)? else {
                return Ok(());
            };
            let Some(runway_true_bearing) = row.get::<_, Option<f64>>(6)? else {
                return Ok(());
            };
            let Some(runway_magnetic_bearing) = row.get::<_, Option<f64>>(7)? else {
                return Ok(());
            };
            let Some(landing_threshold_elevation) = row.get::<_, Option<f64>>(8)? else {
                return Ok(());
            };

            let runway_identifier = format!("RW{}", ident);
            if existing_runways.contains(&(icao.clone(), runway_identifier.clone())) {
                return Ok(());
            }

            let (llz_identifier, llz_mls_gls_category) = get_llz_info(&icao, &ident, ils_map);
            let icao_code = if icao.len() >= 2 {
                icao[..2].to_string()
            } else {
                "UN".to_string()
            };
            out.push(RunwayInsertRow {
                airport_identifier: icao.clone(),
                area_code: area_code_for_icao(&icao_code).to_string(),
                displaced_threshold_distance: 0,
                icao_code: icao_code.clone(),
                landing_threshold_elevation: python_round(landing_threshold_elevation),
                llz_identifier,
                llz_mls_gls_category,
                runway_gradient: 0.0,
                runway_identifier: runway_identifier.clone(),
                runway_latitude,
                runway_length: python_round(runway_length_m * FEET_PER_METER),
                runway_longitude,
                runway_magnetic_bearing,
                runway_true_bearing,
                runway_width: python_round(runway_width_m * FEET_PER_METER),
                surface_code: "CONC".to_string(),
                threshold_crossing_height: 50,
                id: format!("{}{}{}", icao, icao_code, runway_identifier),
            });
            Ok(())
        })
        .map_err(sqlite_error)?;
        Ok(out)
    })();
    conn.close_native();
    result
}

fn bind_runway_row(stmt: &mut rusqlite::Statement<'_>, row: &RunwayInsertRow) -> rusqlite::Result<()> {
    stmt.raw_bind_parameter(1, row.area_code.as_str())?;
    stmt.raw_bind_parameter(2, row.icao_code.as_str())?;
    stmt.raw_bind_parameter(3, row.airport_identifier.as_str())?;
    stmt.raw_bind_parameter(4, row.runway_identifier.as_str())?;
    stmt.raw_bind_parameter(5, row.runway_latitude)?;
    stmt.raw_bind_parameter(6, row.runway_longitude)?;
    stmt.raw_bind_parameter(7, row.runway_gradient)?;
    stmt.raw_bind_parameter(8, row.runway_magnetic_bearing)?;
    stmt.raw_bind_parameter(9, row.runway_true_bearing)?;
    stmt.raw_bind_parameter(10, row.landing_threshold_elevation)?;
    stmt.raw_bind_parameter(11, row.displaced_threshold_distance)?;
    stmt.raw_bind_parameter(12, row.threshold_crossing_height)?;
    stmt.raw_bind_parameter(13, row.runway_length)?;
    stmt.raw_bind_parameter(14, row.runway_width)?;
    match &row.llz_identifier {
        Some(v) => stmt.raw_bind_parameter(15, v.as_str())?,
        None => stmt.raw_bind_parameter(15, rusqlite::types::Null)?,
    }
    match row.llz_mls_gls_category {
        Some(v) => stmt.raw_bind_parameter(16, v)?,
        None => stmt.raw_bind_parameter(16, rusqlite::types::Null)?,
    }
    stmt.raw_bind_parameter(17, row.surface_code.as_str())?;
    stmt.raw_bind_parameter(18, row.id.as_str())?;
    stmt.raw_execute()?;
    Ok(())
}

fn insert_rows(
    conn: &RustSqliteConnection,
    rows: &[RunwayInsertRow],
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
                for row in &rows[start..end] {
                    bind_runway_row(&mut stmt, row)?;
                }
            }
            tx.commit()?;
        }
        Ok(())
    })?;
    Ok(())
}

pub(crate) fn process_runways_to_db(
    nd_db_path: &str,
    path_to_first_csv: &str,
    path_to_second_csv: &str,
    path_to_magvar_csv: &str,
    conn: &RustSqliteConnection,
) -> Result<(usize, usize, usize, Vec<String>)> {
    let first_rows = parse_first_csv(path_to_first_csv)
        .map_err(|err| anyhow!("parse_first_csv failed: {}", err))?;
    let second_by_id = parse_second_csv(path_to_second_csv)
        .map_err(|err| anyhow!("parse_second_csv failed: {}", err))?;
    let magvar_map = parse_magvar_csv(path_to_magvar_csv)
        .map_err(|err| anyhow!("parse_magvar_csv failed: {}", err))?;
    let airport_data = load_airport_data(nd_db_path)
        .map_err(|err| anyhow!("load_airport_data failed: {}", err))?;
    if airport_data.is_empty() {
        return Err(anyhow!(
            "No airport data loaded. Check the master.db3 file."
        ));
    }
    let ils_map =
        load_ils_map(nd_db_path).map_err(|err| anyhow!("load_ils_map failed: {}", err))?;

    conn.execute_statement_native("\n    DELETE FROM tbl_runways WHERE airport_identifier IN ('ZL02', 'ZL03', 'ZW01', 'ZW02');\n    ", &[])
        .map_err(sqlite_error)?;
    conn.execute_statement_native("\n    DELETE FROM tbl_airports WHERE airport_identifier IN ('ZL02', 'ZL03', 'ZW01', 'ZW02');\n    ", &[])
        .map_err(sqlite_error)?;
    conn.execute_statement_native("\n    CREATE TABLE IF NOT EXISTS tbl_runways (\n        area_code TEXT(3),\n        icao_code TEXT(2),\n        airport_identifier TEXT(4) NOT NULL,\n        runway_identifier TEXT(3) NOT NULL,\n        runway_latitude DOUBLE(9),\n        runway_longitude DOUBLE(10),\n        runway_gradient DOUBLE(5),\n        runway_magnetic_bearing DOUBLE(6),\n        runway_true_bearing DOUBLE(7),\n        landing_threshold_elevation INTEGER(5),\n        displaced_threshold_distance INTEGER(4),\n        threshold_crossing_height INTEGER(2),\n        runway_length INTEGER(5),\n        runway_width INTEGER(3),\n        llz_identifier TEXT(4),\n        llz_mls_gls_category TEXT(1),\n        surface_code INTEGER(3),\n        id TEXT(15)\n    );\n    ", &[])
        .map_err(sqlite_error)?;

    let mut existing_runways = fetch_existing_runways(conn)
        .map_err(|err| anyhow!("fetch_existing_runways failed: {}", err))?;
    let (primary_rows, missing_coordinates) = build_primary_rows(
        &first_rows,
        &second_by_id,
        &magvar_map,
        &airport_data,
        &existing_runways,
        &ils_map,
    );
    for row in &primary_rows {
        existing_runways.insert((
            row.airport_identifier.clone(),
            row.runway_identifier.clone(),
        ));
    }

    let supplementary_rows = build_supplementary_rows(nd_db_path, &existing_runways, &ils_map)
        .map_err(|err| anyhow!("build_supplementary_rows failed: {}", err))?;

    insert_rows(conn, &primary_rows)
        .map_err(|err| anyhow!("insert primary runway rows failed: {}", err))?;
    insert_rows(conn, &supplementary_rows)
        .map_err(|err| anyhow!("insert supplementary runway rows failed: {}", err))?;

    let missing_count = missing_coordinates.len();
    let missing_samples = missing_coordinates.into_iter().take(5).collect::<Vec<_>>();
    Ok((
        primary_rows.len(),
        supplementary_rows.len(),
        missing_count,
        missing_samples,
    ))
}

fn sqlite_error(err: rusqlite::Error) -> anyhow::Error {
    anyhow!(err.to_string())
}
