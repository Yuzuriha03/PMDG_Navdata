use crate::core::db::RustSqliteConnection;
use crate::core::magnetic::batch_get_magnetic_variations_internal;
use crate::enroute::airways::parse_dms_list;
use anyhow::{anyhow, Result};
use csv::{ReaderBuilder, StringRecord, Trim};
use encoding_rs::Encoding;
use pinyin::ToPinyin;
use std::collections::HashSet;
use std::fs;

#[derive(Clone)]
struct AirportCsvRow {
    code_id: String,
    code_iata: Option<String>,
    txt_name: Option<String>,
    latitude: f64,
    longitude: f64,
    val_elev: Option<f64>,
    val_transition_alt: Option<f64>,
    val_transition_level: Option<f64>,
}

#[derive(Clone)]
struct AirportInsertRow {
    airport_identifier: String,
    airport_name: String,
    airport_ref_latitude: f64,
    airport_ref_longitude: f64,
    airport_type: String,
    area_code: String,
    ata_iata_code: Option<String>,
    city: String,
    continent: String,
    country_3letter: String,
    country: String,
    elevation: Option<i64>,
    fuel: String,
    icao_code: String,
    ifr_capability: String,
    longest_runway_surface_code: String,
    magnetic_variation: f64,
    speed_limit_altitude: i64,
    speed_limit: i64,
    state_2letter: String,
    state: String,
    time_zone: String,
    transition_altitude: Option<i64>,
    transition_level: Option<i64>,
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
        .ok_or_else(|| anyhow!("required airport CSV column missing: {}", column))
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

fn round_feet(value_meters: Option<f64>) -> Option<i64> {
    value_meters.map(|value| (value * 3.28084).round() as i64)
}

fn round_feet_hundreds(value_meters: Option<f64>) -> Option<i64> {
    value_meters.map(|value| (((value * 3.28084) / 100.0).round() * 100.0) as i64)
}

fn special_phrase_pinyin(value: &str) -> Option<&'static str> {
    match value.trim() {
        "吕梁" => Some("LÜLIANG"),
        "三女河" => Some("SANNÜHE"),
        "仙女山" => Some("XIANNÜSHAN"),
        "重庆" => Some("CHONGQING"),
        "龟兹" => Some("QIUCI"),
        "长安" => Some("CHANGAN"),
        "长白山" => Some("CHANGBAISHAN"),
        "朝阳" => Some("CHAOYANG"),
        _ => None,
    }
}

fn to_upper_pinyin(value: &str) -> String {
    if let Some(mapped) = special_phrase_pinyin(value) {
        return mapped.to_string();
    }

    let mut out = String::new();
    for ch in value.chars() {
        if ch.is_whitespace() {
            continue;
        }
        match ch {
            '吕' => {
                out.push('L');
                out.push('Ü');
                continue;
            }
            '女' => {
                out.push('N');
                out.push('Ü');
                continue;
            }
            _ => {}
        }
        if let Some(pinyin) = ch.to_pinyin() {
            out.push_str(pinyin.plain());
        } else {
            out.extend(ch.to_uppercase());
        }
    }
    out.to_uppercase()
}

fn parse_names(txt_name: Option<&str>) -> Result<(String, String)> {
    let Some(txt_name) = txt_name.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(("UNKNOWN".to_string(), "UNKNOWN".to_string()));
    };

    let mut parts = Vec::new();
    for part in txt_name
        .split('/')
        .map(str::trim)
        .filter(|part| !part.is_empty())
    {
        parts.push(to_upper_pinyin(part));
    }

    if parts.is_empty() {
        return Ok(("UNKNOWN".to_string(), "UNKNOWN".to_string()));
    }

    let city = parts[0].clone();
    let airport = if parts.len() > 1 {
        parts[1].clone()
    } else {
        city.clone()
    };
    Ok((city, airport))
}

fn parse_airport_csv(csv_file: &str) -> Result<Vec<AirportCsvRow>> {
    let content = decode_gb18030_file(csv_file)?;
    let mut reader = ReaderBuilder::new()
        .trim(Trim::All)
        .flexible(true)
        .from_reader(content.as_bytes());
    let headers = reader
        .headers()
        .map_err(|err| anyhow!("failed to read airport CSV headers: {}", err))?
        .clone();

    let code_id_idx = required_index(&headers, "CODE_ID")?;
    let txt_name_idx = optional_index(&headers, "TXT_NAME");
    let code_iata_idx = optional_index(&headers, "CODE_IATA");
    let geo_lat_idx = required_index(&headers, "GEO_LAT_ACCURACY")?;
    let geo_lon_idx = required_index(&headers, "GEO_LONG_ACCURACY")?;
    let elev_idx = optional_index(&headers, "VAL_ELEV");
    let ta_idx = optional_index(&headers, "VAL_TRANSITION_ALT");
    let tl_idx = optional_index(&headers, "VAL_TRANSITION_LEVEL");

    let mut code_ids: Vec<String> = Vec::new();
    let mut code_iatas: Vec<Option<String>> = Vec::new();
    let mut txt_names: Vec<Option<String>> = Vec::new();
    let mut lat_inputs: Vec<Option<String>> = Vec::new();
    let mut lon_inputs: Vec<Option<String>> = Vec::new();
    let mut elevations: Vec<Option<f64>> = Vec::new();
    let mut transition_alts: Vec<Option<f64>> = Vec::new();
    let mut transition_levels: Vec<Option<f64>> = Vec::new();

    for record in reader.records() {
        let record = record.map_err(|err| anyhow!("failed to parse airport CSV row: {}", err))?;
        let code_id = record
            .get(code_id_idx)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string);
        let Some(code_id) = code_id else {
            continue;
        };

        code_ids.push(code_id);
        code_iatas.push(optional_string(&record, code_iata_idx));
        txt_names.push(optional_string(&record, txt_name_idx));
        lat_inputs.push(
            record
                .get(geo_lat_idx)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string),
        );
        lon_inputs.push(
            record
                .get(geo_lon_idx)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string),
        );
        elevations.push(optional_f64(&record, elev_idx));
        transition_alts.push(optional_f64(&record, ta_idx));
        transition_levels.push(optional_f64(&record, tl_idx));
    }

    let latitudes = parse_dms_list(lat_inputs.clone());
    let longitudes = parse_dms_list(lon_inputs.clone());

    let mut rows = Vec::new();
    for index in 0..code_ids.len() {
        let (Some(latitude), Some(longitude)) = (latitudes[index], longitudes[index]) else {
            continue;
        };
        rows.push(AirportCsvRow {
            code_id: code_ids[index].clone(),
            code_iata: code_iatas[index].clone(),
            txt_name: txt_names[index].clone(),
            latitude,
            longitude,
            val_elev: elevations[index],
            val_transition_alt: transition_alts[index],
            val_transition_level: transition_levels[index],
        });
    }

    Ok(rows)
}

fn build_airport_insert_rows(rows: &[AirportCsvRow]) -> Result<Vec<AirportInsertRow>> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }

    let coordinates: Vec<(f64, f64)> = rows
        .iter()
        .map(|row| (row.latitude, row.longitude))
        .collect();
    let declinations = batch_get_magnetic_variations_internal(&coordinates)?;
    let mut out = Vec::with_capacity(rows.len());

    for (row, magnetic_variation) in rows.iter().zip(declinations) {
        let (city, airport_name) = parse_names(row.txt_name.as_deref())?;
        out.push(AirportInsertRow {
            airport_identifier: row.code_id.clone(),
            airport_name,
            airport_ref_latitude: row.latitude,
            airport_ref_longitude: row.longitude,
            airport_type: "C".to_string(),
            area_code: "EEU".to_string(),
            ata_iata_code: row.code_iata.clone(),
            city,
            continent: "ASIA".to_string(),
            country_3letter: "CHN".to_string(),
            country: "CHINA".to_string(),
            elevation: round_feet(row.val_elev),
            fuel: "NNNNNNNNNYNNNN".to_string(),
            icao_code: row.code_id.chars().take(2).collect(),
            ifr_capability: "Y".to_string(),
            longest_runway_surface_code: "H".to_string(),
            magnetic_variation,
            speed_limit_altitude: 10000,
            speed_limit: 250,
            state_2letter: String::new(),
            state: String::new(),
            time_zone: "H00".to_string(),
            transition_altitude: round_feet_hundreds(row.val_transition_alt),
            transition_level: round_feet_hundreds(row.val_transition_level),
        });
    }

    Ok(out)
}

fn get_existing_airports(conn: &RustSqliteConnection) -> Result<HashSet<String>> {
    let mut out = HashSet::new();
    conn.query_each_native(
        "SELECT airport_identifier FROM tbl_pa_airports",
        &[],
        |row| {
            out.insert(row.get::<_, String>(0)?);
            Ok(())
        },
    )
    .map_err(sqlite_error)?;
    Ok(out)
}

fn bind_airport_row(
    stmt: &mut rusqlite::Statement<'_>,
    row: &AirportInsertRow,
) -> rusqlite::Result<()> {
    stmt.raw_bind_parameter(1, row.airport_identifier.as_str())?;
    stmt.raw_bind_parameter(2, row.airport_name.as_str())?;
    stmt.raw_bind_parameter(3, row.airport_ref_latitude)?;
    stmt.raw_bind_parameter(4, row.airport_ref_longitude)?;
    stmt.raw_bind_parameter(5, row.airport_type.as_str())?;
    stmt.raw_bind_parameter(6, row.area_code.as_str())?;
    match &row.ata_iata_code {
        Some(v) => stmt.raw_bind_parameter(7, v.as_str())?,
        None => stmt.raw_bind_parameter(7, rusqlite::types::Null)?,
    }
    stmt.raw_bind_parameter(8, row.city.as_str())?;
    stmt.raw_bind_parameter(9, row.continent.as_str())?;
    stmt.raw_bind_parameter(10, row.country_3letter.as_str())?;
    stmt.raw_bind_parameter(11, row.country.as_str())?;
    match row.elevation {
        Some(v) => stmt.raw_bind_parameter(12, v)?,
        None => stmt.raw_bind_parameter(12, rusqlite::types::Null)?,
    }
    stmt.raw_bind_parameter(13, row.fuel.as_str())?;
    stmt.raw_bind_parameter(14, row.icao_code.as_str())?;
    stmt.raw_bind_parameter(15, row.ifr_capability.as_str())?;
    stmt.raw_bind_parameter(16, row.longest_runway_surface_code.as_str())?;
    stmt.raw_bind_parameter(17, row.magnetic_variation)?;
    stmt.raw_bind_parameter(18, row.speed_limit_altitude)?;
    stmt.raw_bind_parameter(19, row.speed_limit)?;
    stmt.raw_bind_parameter(20, row.state_2letter.as_str())?;
    stmt.raw_bind_parameter(21, row.state.as_str())?;
    stmt.raw_bind_parameter(22, row.time_zone.as_str())?;
    match row.transition_altitude {
        Some(v) => stmt.raw_bind_parameter(23, v)?,
        None => stmt.raw_bind_parameter(23, rusqlite::types::Null)?,
    }
    match row.transition_level {
        Some(v) => stmt.raw_bind_parameter(24, v)?,
        None => stmt.raw_bind_parameter(24, rusqlite::types::Null)?,
    }
    stmt.raw_execute()?;
    Ok(())
}

fn insert_airport_rows(conn: &RustSqliteConnection, rows: &[AirportInsertRow]) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let sql = "INSERT OR IGNORE INTO tbl_pa_airports (airport_identifier, airport_name, airport_ref_latitude, airport_ref_longitude, airport_type, area_code, ata_iata_code, city, continent, country_3letter, country, elevation, fuel, icao_code, ifr_capability, longest_runway_surface_code, magnetic_variation, speed_limit_altitude, speed_limit, state_2letter, state, time_zone, transition_altitude, transition_level) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    conn.with_connection_native(|raw_conn| {
        let actual_batch = 500; // heuristics
        for start in (0..rows.len()).step_by(actual_batch) {
            let end = (start + actual_batch).min(rows.len());
            let tx = raw_conn.unchecked_transaction()?;
            {
                let mut stmt = tx.prepare(sql)?;
                for row in &rows[start..end] {
                    bind_airport_row(&mut stmt, row)?;
                }
            }
            tx.commit()?;
        }
        Ok(())
    })?;
    Ok(())
}

fn insert_zlyx_if_missing(
    master_db_path: &str,
    existing_airports: &HashSet<String>,
    conn: &RustSqliteConnection,
) -> Result<bool> {
    if existing_airports.contains("ZLYX") {
        return Ok(false);
    }

    let master_conn =
        RustSqliteConnection::open_native(master_db_path, 30).map_err(sqlite_error)?;
    let result: Result<Option<AirportInsertRow>> = (|| {
        let mut zlyx = None;
        master_conn
            .query_each_native(
                "SELECT ICAO, IATA, name, latitude, longitude, alt, mag_var, ta, tl FROM airport WHERE ICAO='ZLYX'",
                &[],
                |row| {
                    if zlyx.is_some() {
                        return Ok(());
                    }

                    let airport_identifier: String = row.get(0)?;
                    let iata_code: Option<String> = row.get(1)?;
                    let airport_name: String = row.get(2)?;
                    let latitude: f64 = row.get(3)?;
                    let longitude: f64 = row.get(4)?;
                    let elevation: Option<i64> = row.get(5)?;
                    let magnetic_variation: f64 = row.get(6)?;
                    let transition_altitude: Option<i64> = row.get(7)?;
                    let transition_level: Option<i64> = row.get(8)?;

                    let ata_iata_code = match iata_code {
                        Some(value) => Some(value),
                        None => Some(String::new()),
                    };
                    zlyx = Some(AirportInsertRow {
                        airport_identifier: airport_identifier.clone(),
                        airport_name: airport_name.clone(),
                        airport_ref_latitude: latitude,
                        airport_ref_longitude: longitude,
                        airport_type: "C".to_string(),
                        area_code: "EEU".to_string(),
                        ata_iata_code,
                        city: airport_name,
                        continent: "ASIA".to_string(),
                        country_3letter: "CHN".to_string(),
                        country: "CHINA".to_string(),
                        elevation,
                        fuel: "NNNNNNNNNYNNNN".to_string(),
                        icao_code: airport_identifier.chars().take(2).collect(),
                        ifr_capability: "Y".to_string(),
                        longest_runway_surface_code: "H".to_string(),
                        magnetic_variation,
                        speed_limit_altitude: 10000,
                        speed_limit: 250,
                        state_2letter: String::new(),
                        state: String::new(),
                        time_zone: "H00".to_string(),
                        transition_altitude: transition_altitude.or(Some(0)),
                        transition_level: transition_level.or(Some(0)),
                    });
                    Ok(())
                },
            )
            .map_err(sqlite_error)?;
        Ok(zlyx)
    })();
    master_conn.close_native();

    let Some(zlyx) = result? else {
        return Ok(false);
    };
    insert_airport_rows(conn, &[zlyx])?;
    Ok(true)
}

pub(crate) fn process_airports_to_db(
    csv_file: &str,
    master_db_path: &str,
    conn: &RustSqliteConnection,
) -> Result<(usize, bool)> {
    let rows =
        parse_airport_csv(csv_file).map_err(|err| anyhow!("parse_airport_csv failed: {}", err))?;
    let insert_rows = build_airport_insert_rows(&rows)
        .map_err(|err| anyhow!("build_airport_insert_rows failed: {}", err))?;

    conn.execute_statement_native(
            "CREATE TABLE IF NOT EXISTS tbl_pa_airports (airport_identifier TEXT PRIMARY KEY, airport_name TEXT, airport_ref_latitude REAL, airport_ref_longitude REAL, airport_type TEXT, area_code TEXT, ata_iata_code TEXT, city TEXT, continent TEXT, country_3letter TEXT, country TEXT, elevation INTEGER, fuel TEXT, icao_code TEXT, ifr_capability TEXT, longest_runway_surface_code TEXT, magnetic_variation REAL, speed_limit_altitude INTEGER, speed_limit INTEGER, state_2letter TEXT, state TEXT, time_zone TEXT, transition_altitude INTEGER, transition_level INTEGER, UNIQUE(icao_code, airport_identifier))",
            &[],
        )
        .map_err(sqlite_error)?;

    let existing_airports = get_existing_airports(conn)
        .map_err(|err| anyhow!("get_existing_airports failed: {}", err))?;
    let new_rows: Vec<AirportInsertRow> = insert_rows
        .into_iter()
        .filter(|row| !existing_airports.contains(&row.airport_identifier))
        .collect();

    insert_airport_rows(conn, &new_rows)
        .map_err(|err| anyhow!("insert_airport_rows failed: {}", err))?;
    let inserted_zlyx = insert_zlyx_if_missing(master_db_path, &existing_airports, conn)
        .map_err(|err| anyhow!("insert_zlyx_if_missing failed: {}", err))?;
    Ok((new_rows.len(), inserted_zlyx))
}

fn sqlite_error(err: rusqlite::Error) -> anyhow::Error {
    anyhow!(err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_chinese_ascii_and_mixed_names_to_upper_pinyin() {
        assert_eq!(to_upper_pinyin("北京"), "BEIJING");
        assert_eq!(to_upper_pinyin("abc-123"), "ABC-123");
        assert_eq!(to_upper_pinyin("北京 T3"), "BEIJINGT3");
        assert_eq!(to_upper_pinyin("吕梁"), "LÜLIANG");
        assert_eq!(to_upper_pinyin("女"), "NÜ");
    }

    #[test]
    fn parses_airport_names_like_legacy_python_logic() {
        assert_eq!(
            parse_names(Some("北京/机场")).unwrap(),
            ("BEIJING".to_string(), "JICHANG".to_string())
        );
        assert_eq!(
            parse_names(Some("上海虹桥")).unwrap(),
            (
                "SHANGHAIHONGQIAO".to_string(),
                "SHANGHAIHONGQIAO".to_string()
            )
        );
        assert_eq!(
            parse_names(Some("重庆/仙女山")).unwrap(),
            ("CHONGQING".to_string(), "XIANNÜSHAN".to_string())
        );
        assert_eq!(
            parse_names(Some(" ")).unwrap(),
            ("UNKNOWN".to_string(), "UNKNOWN".to_string())
        );
    }

    #[test]
    fn matches_phrase_level_overrides_for_airport_dataset() {
        assert_eq!(to_upper_pinyin("三女河"), "SANNÜHE");
        assert_eq!(to_upper_pinyin("重庆"), "CHONGQING");
        assert_eq!(to_upper_pinyin("龟兹"), "QIUCI");
        assert_eq!(to_upper_pinyin("长安"), "CHANGAN");
        assert_eq!(to_upper_pinyin("长白山"), "CHANGBAISHAN");
        assert_eq!(to_upper_pinyin("朝阳"), "CHAOYANG");
    }
}
