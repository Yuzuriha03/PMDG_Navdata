use crate::core::db::RustSqliteConnection;
use crate::core::geo::{haversine_km, magnetic_bearing, KM_TO_NM};
use crate::core::magnetic::batch_get_magnetic_variations_internal;
use crate::core::matchers::IcaoCodeResolver;
use anyhow::{anyhow, Result};
use encoding_rs::GBK;
use rusqlite::types::Value as SqlValue;
use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Cursor};

const DAT_READER_CAPACITY: usize = 256 * 1024;
const ENROUTE_AIRWAYS_TABLE: &str = "tbl_enroute_airways";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AirwayCodeType {
    DesignatedPoint,
    VorDme,
    Ndb,
    Other,
}

impl AirwayCodeType {
    fn parse(value: &str) -> Self {
        match value {
            "DESIGNATED_POINT" | "地名点" => Self::DesignatedPoint,
            "VORDME" => Self::VorDme,
            "NDB" => Self::Ndb,
            _ => Self::Other,
        }
    }

    fn resolver_code(self) -> Option<&'static str> {
        match self {
            Self::DesignatedPoint => Some("DESIGNATED_POINT"),
            Self::VorDme => Some("VORDME"),
            Self::Ndb => Some("NDB"),
            Self::Other => None,
        }
    }
}

#[derive(Clone)]
struct ParsedAirwayCsvRow {
    route_identifier: Box<str>,
    start_waypoint: Box<str>,
    start_code_type: AirwayCodeType,
    direction_restriction: Option<Box<str>>,
    outbound_course: f64,
    inbound_distance: f64,
    end_waypoint: Option<Box<str>>,
    end_code_type: Option<AirwayCodeType>,
}

#[derive(Clone)]
struct AirwayRecord {
    area_code: String,
    crusing_table_identifier: Option<String>,
    direction_restriction: Option<String>,
    flightlevel: String,
    icao_code: Option<String>,
    inbound_course: f64,
    inbound_distance: f64,
    maximum_altitude: Option<i64>,
    minimum_altitude1: Option<i64>,
    minimum_altitude2: Option<f64>,
    outbound_course: f64,
    route_identifier: String,
    route_type: String,
    seqno: i64,
    waypoint_description_code: String,
    waypoint_identifier: String,
    waypoint_latitude: Option<f64>,
    waypoint_longitude: Option<f64>,
    id: Option<String>,
}

type AirwayAidType = &'static str;
type AirwayCoord = Option<(f64, f64)>;
type AirwayCoordMap = HashMap<String, AirwayCoord>;
type AirwaySegmentTask = (
    usize,
    usize,
    Option<f64>,
    Option<f64>,
    Option<f64>,
    Option<f64>,
);
type AirwaySegmentMetric = (usize, usize, f64, f64);
type CsvRoutePoint = (String, usize);
type DbRoutePoint = (String, usize, i64);
type CsvRouteMap = HashMap<String, Vec<CsvRoutePoint>>;
type DbRouteMap = HashMap<String, Vec<DbRoutePoint>>;
type ExistingRouteWaypoint = (String, String);
type MaxSeqItem = (String, i64);
type AirwayEndCsvLastRow = (String, Option<String>, Option<f64>, Option<AirwayCodeType>);
type AirwayEndAppendPlanRow = (
    String,
    String,
    f64,
    Option<AirwayCodeType>,
    String,
    String,
    i64,
);
type AirwayDirectionEndpoints = HashMap<String, String>;
type AirwayDirectionStartMap = HashMap<String, AirwayDirectionEndpoints>;
type AirwayDirectionRouteMap = HashMap<String, AirwayDirectionStartMap>;

#[derive(Clone, Copy, Eq, PartialEq)]
enum RowSource {
    Csv,
    Db,
}

type AirwayRowPlanEntry = (RowSource, usize, Option<i64>);
type AirwayMergePlan = (Vec<AirwayRowPlanEntry>, Vec<String>, Vec<String>);

#[derive(Default)]
struct AirwayCoordCache {
    fix: AirwayCoordMap,
    vhf: AirwayCoordMap,
    ndb: AirwayCoordMap,
}

impl AirwayCoordCache {
    fn get(&self, aid_type: AirwayAidType, identifier: &str) -> AirwayCoord {
        match aid_type {
            "fix" => self.fix.get(identifier).copied().flatten(),
            "vhf" => self.vhf.get(identifier).copied().flatten(),
            "ndb" => self.ndb.get(identifier).copied().flatten(),
            _ => None,
        }
    }

    fn map_mut(&mut self, aid_type: AirwayAidType) -> Option<&mut AirwayCoordMap> {
        match aid_type {
            "fix" => Some(&mut self.fix),
            "vhf" => Some(&mut self.vhf),
            "ndb" => Some(&mut self.ndb),
            _ => None,
        }
    }
}

#[derive(Default)]
struct ResolverIdentifierSets {
    fix_identifiers: HashSet<Box<str>>,
    vor_dme_identifiers: HashSet<Box<str>>,
    ndb_identifiers: HashSet<Box<str>>,
}

fn merge_airway_route_order(db_rows: &[DbRoutePoint], csv_rows: &[CsvRoutePoint]) -> Vec<String> {
    let db_points: Vec<&str> = db_rows.iter().map(|(wp, _, _)| wp.as_str()).collect();
    let csv_points: Vec<&str> = csv_rows.iter().map(|(wp, _)| wp.as_str()).collect();
    let db_set: HashSet<&str> = db_points.iter().copied().collect();
    let csv_set: HashSet<&str> = csv_points.iter().copied().collect();
    if !db_set.iter().any(|wp| csv_set.contains(wp)) {
        return Vec::new();
    }

    let mut merged_order: Vec<String> = Vec::new();
    let mut merged_set: HashSet<&str> = HashSet::new();

    let mut csv_index: HashMap<&str, usize> = HashMap::new();
    for (idx, wp) in csv_points.iter().enumerate() {
        csv_index.entry(*wp).or_insert(idx);
    }

    let db_len = db_points.len();
    let mut common_flags = vec![false; db_len];
    for (flag, wp) in common_flags.iter_mut().zip(db_points.iter()) {
        *flag = csv_set.contains(wp);
    }

    let mut next_common_idx = vec![None; db_len];
    let mut next_idx: Option<usize> = None;
    for i in (0..db_len).rev() {
        next_common_idx[i] = next_idx;
        if common_flags[i] {
            next_idx = Some(i);
        }
    }

    for (i, wp) in db_points.iter().enumerate() {
        if merged_set.insert(*wp) {
            merged_order.push((*wp).to_string());
        }

        if !common_flags[i] {
            continue;
        }
        let Some(nxt_idx) = next_common_idx[i] else {
            continue;
        };
        let next_common = db_points[nxt_idx];

        let Some(idx_csv_current) = csv_index.get(wp) else {
            continue;
        };
        let Some(idx_csv_next) = csv_index.get(next_common) else {
            continue;
        };

        if idx_csv_current < idx_csv_next {
            for m in &csv_points[idx_csv_current + 1..*idx_csv_next] {
                if merged_set.insert(*m) {
                    merged_order.push((*m).to_string());
                }
            }
        } else if idx_csv_next < idx_csv_current {
            for m in csv_points[*idx_csv_next + 1..*idx_csv_current].iter().rev() {
                if merged_set.insert(*m) {
                    merged_order.push((*m).to_string());
                }
            }
        }
    }

    merged_order
}

fn build_airway_segment_metrics(
    tasks: Vec<AirwaySegmentTask>,
    declinations: Vec<f64>,
) -> Result<Vec<AirwaySegmentMetric>> {
    if tasks.len() != declinations.len() {
        return Err(anyhow!("tasks and declinations length mismatch"));
    }

    let mut out: Vec<AirwaySegmentMetric> = Vec::with_capacity(tasks.len());

    for (idx, (current_idx, next_idx, curr_lat, curr_lon, next_lat, next_lon)) in
        tasks.into_iter().enumerate()
    {
        let (inbound_distance, outbound_course) =
            if let (Some(curr_lat), Some(curr_lon), Some(next_lat), Some(next_lon)) =
                (curr_lat, curr_lon, next_lat, next_lon)
            {
                let declination = declinations[idx];
                let km = haversine_km(curr_lon, curr_lat, next_lon, next_lat);
                let nm = ((km * KM_TO_NM) * 10.0).round() / 10.0;
                let outbound =
                    magnetic_bearing(curr_lat, curr_lon, next_lat, next_lon, declination).round();
                (nm, outbound)
            } else {
                (0.0, 0.0)
            };

        out.push((current_idx, next_idx, inbound_distance, outbound_course));
    }

    Ok(out)
}

fn build_airway_merge_plan(
    route_order: &[String],
    csv_map: &CsvRouteMap,
    db_map: &DbRouteMap,
) -> AirwayMergePlan {
    let mut row_plan: Vec<AirwayRowPlanEntry> = Vec::new();
    let mut routes_to_delete: Vec<String> = Vec::new();
    let mut routes_with_missing: Vec<String> = Vec::new();

    for route in route_order {
        routes_to_delete.push(route.clone());

        let Some(csv_rows) = csv_map.get(route.as_str()) else {
            continue;
        };
        let db_rows_opt = db_map.get(route.as_str());

        let csv_points: HashSet<&str> = csv_rows.iter().map(|(wp, _)| wp.as_str()).collect();
        let has_overlap = db_rows_opt
            .map(|rows| {
                rows.iter()
                    .any(|(wp, _, _)| csv_points.contains(wp.as_str()))
            })
            .unwrap_or(false);

        if !has_overlap {
            let max_seqno = db_rows_opt
                .and_then(|rows| rows.iter().map(|(_, _, seq)| *seq).max())
                .unwrap_or(1000);

            if let Some(db_rows) = db_rows_opt {
                for (_, db_idx, _) in db_rows {
                    row_plan.push((RowSource::Db, *db_idx, None));
                }
            }

            for (i, (_, csv_idx)) in csv_rows.iter().enumerate() {
                let seqno = max_seqno + 5 * (i as i64 + 1);
                row_plan.push((RowSource::Csv, *csv_idx, Some(seqno)));
            }
            continue;
        }

        let Some(db_rows) = db_rows_opt else {
            continue;
        };
        let merged_order = merge_airway_route_order(db_rows, csv_rows);
        let merged_set: HashSet<&str> = merged_order.iter().map(String::as_str).collect();

        let mut csv_lookup_first: HashMap<&str, usize> = HashMap::new();
        for (wp, idx) in csv_rows {
            csv_lookup_first.entry(wp.as_str()).or_insert(*idx);
        }

        let mut db_lookup_first: HashMap<&str, usize> = HashMap::new();
        for (wp, idx, _) in db_rows {
            db_lookup_first.entry(wp.as_str()).or_insert(*idx);
        }

        let mut merged_sources: Vec<(RowSource, usize)> = Vec::with_capacity(merged_order.len());
        for wp in merged_order.iter().map(String::as_str) {
            if let Some(csv_idx) = csv_lookup_first.get(wp) {
                merged_sources.push((RowSource::Csv, *csv_idx));
            } else if let Some(db_idx) = db_lookup_first.get(wp) {
                merged_sources.push((RowSource::Db, *db_idx));
            }
        }

        for (wp, csv_idx) in csv_rows {
            if !merged_set.contains(wp.as_str()) {
                merged_sources.push((RowSource::Csv, *csv_idx));
            }
        }

        for (i, (source, idx)) in merged_sources.into_iter().enumerate() {
            row_plan.push((source, idx, Some(1000 + i as i64 * 5)));
        }
        routes_with_missing.push(route.clone());
    }

    (row_plan, routes_to_delete, routes_with_missing)
}

fn parse_dms_to_decimal(dms: &str) -> Option<f64> {
    if dms.len() < 7 {
        return None;
    }
    let mut chars = dms.chars();
    let direction = chars.next()?;

    match direction {
        'N' | 'S' => {
            let deg: f64 = dms.get(1..3)?.parse().ok()?;
            let min: f64 = dms.get(3..5)?.parse().ok()?;
            let sec: f64 = dms.get(5..)?.parse().ok()?;
            let mut decimal = deg + min / 60.0 + sec / 3600.0;
            if direction == 'S' {
                decimal = -decimal;
            }
            Some((decimal * 100_000_000.0).round() / 100_000_000.0)
        }
        'E' | 'W' => {
            let deg: f64 = dms.get(1..4)?.parse().ok()?;
            let min: f64 = dms.get(4..6)?.parse().ok()?;
            let sec: f64 = dms.get(6..)?.parse().ok()?;
            let mut decimal = deg + min / 60.0 + sec / 3600.0;
            if direction == 'W' {
                decimal = -decimal;
            }
            Some((decimal * 100_000_000.0).round() / 100_000_000.0)
        }
        _ => None,
    }
}

fn next_ascii_field<'a>(line: &'a str, cursor: &mut usize) -> Option<&'a str> {
    let bytes = line.as_bytes();
    let len = bytes.len();

    while *cursor < len && bytes[*cursor].is_ascii_whitespace() {
        *cursor += 1;
    }
    if *cursor >= len {
        return None;
    }

    let start = *cursor;
    while *cursor < len && !bytes[*cursor].is_ascii_whitespace() {
        *cursor += 1;
    }

    Some(&line[start..*cursor])
}

fn trim_csv_field(field: &str) -> &str {
    field.trim_matches(|ch| matches!(ch, ' ' | '\t' | '\r' | '\n'))
}

fn parse_csv_header_indices_simple(line: &str) -> Result<[usize; 8]> {
    let header = line.strip_prefix('\u{feff}').unwrap_or(line);
    let mut index_map = HashMap::new();
    for (idx, field) in header
        .trim_end_matches(|ch| matches!(ch, '\r' | '\n'))
        .split(',')
        .enumerate()
    {
        index_map.insert(trim_csv_field(field), idx);
    }

    let required = |name: &str| {
        index_map
            .get(name)
            .copied()
            .ok_or_else(|| anyhow!("required airway CSV column missing: {}", name))
    };
    let optional = |name: &str| index_map.get(name).copied().unwrap_or(usize::MAX);

    Ok([
        required("TXT_DESIG")?,
        required("CODE_POINT_START")?,
        required("CODE_TYPE_START")?,
        optional("CODE_DIR"),
        optional("CODE_POINT_END"),
        optional("CODE_TYPE_END"),
        optional("VAL_MAG_TRACK"),
        optional("VAL_LEN"),
    ])
}

fn extract_csv_fields_simple<'a, const N: usize>(
    line: &'a str,
    indices: &[usize; N],
) -> [Option<&'a str>; N] {
    let trimmed = line.trim_end_matches(|ch| matches!(ch, '\r' | '\n'));
    let max_target = indices
        .iter()
        .copied()
        .filter(|index| *index != usize::MAX)
        .max()
        .unwrap_or(0);
    let mut out: [Option<&'a str>; N] = [None; N];
    let mut field_index = 0usize;
    let mut start = 0usize;

    loop {
        let end = match trimmed[start..].find(',') {
            Some(offset) => start + offset,
            None => trimmed.len(),
        };

        for (slot, target_index) in indices.iter().enumerate() {
            if *target_index == field_index {
                out[slot] = Some(trim_csv_field(&trimmed[start..end]));
            }
        }

        if end == trimmed.len() || field_index >= max_target {
            break;
        }

        field_index += 1;
        start = end + 1;
    }

    out
}

fn field_value(field: Option<&str>) -> Box<str> {
    field.unwrap_or("").into()
}

fn optional_field_value(field: Option<&str>) -> Option<Box<str>> {
    field.filter(|value| !value.is_empty()).map(Into::into)
}

fn field_f64_or_default(field: Option<&str>, default: f64) -> f64 {
    field
        .filter(|value| !value.is_empty())
        .and_then(|value| value.parse::<f64>().ok())
        .unwrap_or(default)
}

fn parse_airway_csv_rows_simple(csv_file: &str) -> Result<Vec<ParsedAirwayCsvRow>> {
    let content = read_text_gbk(csv_file)?;
    parse_airway_csv_rows_simple_from_bufread(Cursor::new(content))
}

fn parse_airway_csv_rows_simple_from_bufread<R: BufRead>(
    mut reader: R,
) -> Result<Vec<ParsedAirwayCsvRow>> {
    let mut line = String::new();
    if reader.read_line(&mut line)? == 0 {
        return Ok(Vec::new());
    }

    let [route_idx, start_wp_idx, start_code_idx, direction_idx, end_wp_idx, end_code_idx, mag_track_idx, len_idx] =
        parse_csv_header_indices_simple(&line)?;

    let mut out = Vec::new();
    loop {
        line.clear();
        if reader.read_line(&mut line)? == 0 {
            break;
        }

        let [route, start_wp, start_code, direction, end_wp, end_code, mag_track, length] =
            extract_csv_fields_simple(
                &line,
                &[
                    route_idx,
                    start_wp_idx,
                    start_code_idx,
                    direction_idx,
                    end_wp_idx,
                    end_code_idx,
                    mag_track_idx,
                    len_idx,
                ],
            );

        out.push(ParsedAirwayCsvRow {
            route_identifier: field_value(route),
            start_waypoint: field_value(start_wp),
            start_code_type: AirwayCodeType::parse(start_code.unwrap_or("")),
            direction_restriction: optional_field_value(direction),
            outbound_course: field_f64_or_default(mag_track, 0.0),
            inbound_distance: ((field_f64_or_default(length, 0.0) * KM_TO_NM) * 100.0).round()
                / 100.0,
            end_waypoint: optional_field_value(end_wp),
            end_code_type: end_code
                .filter(|value| !value.is_empty())
                .map(AirwayCodeType::parse),
        });
    }

    Ok(out)
}

fn read_text_gbk(file_path: &str) -> Result<String> {
    let bytes =
        fs::read(file_path).map_err(|err| anyhow!("failed to read file {}: {}", file_path, err))?;
    let (decoded, _, _) = GBK.decode(&bytes);
    Ok(decoded.into_owned())
}

fn open_dat_reader(file_path: &str) -> Result<BufReader<File>> {
    let file =
        File::open(file_path).map_err(|err| anyhow!("failed to open {}: {}", file_path, err))?;
    Ok(BufReader::with_capacity(DAT_READER_CAPACITY, file))
}

fn collect_airways_resolver_identifiers(rows: &[ParsedAirwayCsvRow]) -> ResolverIdentifierSets {
    let mut identifiers = ResolverIdentifierSets::default();

    for row in rows {
        match row.start_code_type {
            AirwayCodeType::DesignatedPoint if !row.start_waypoint.is_empty() => {
                identifiers
                    .fix_identifiers
                    .insert(row.start_waypoint.as_ref().into());
            }
            AirwayCodeType::VorDme if !row.start_waypoint.is_empty() => {
                identifiers
                    .vor_dme_identifiers
                    .insert(row.start_waypoint.as_ref().into());
            }
            AirwayCodeType::Ndb if !row.start_waypoint.is_empty() => {
                identifiers
                    .ndb_identifiers
                    .insert(row.start_waypoint.as_ref().into());
            }
            _ => {}
        }

        let Some(end_waypoint) = row
            .end_waypoint
            .as_deref()
            .filter(|value| !value.is_empty())
        else {
            continue;
        };
        match row.end_code_type {
            Some(AirwayCodeType::DesignatedPoint) => {
                identifiers.fix_identifiers.insert(end_waypoint.into());
            }
            Some(AirwayCodeType::VorDme) => {
                identifiers.vor_dme_identifiers.insert(end_waypoint.into());
            }
            Some(AirwayCodeType::Ndb) => {
                identifiers.ndb_identifiers.insert(end_waypoint.into());
            }
            _ => {}
        }
    }

    identifiers
}

fn load_earth_fix_items(
    file_path: &str,
    required_identifiers: &HashSet<Box<str>>,
) -> Result<Vec<(String, String)>> {
    let allowed = [
        "ZB", "ZS", "ZJ", "ZG", "ZY", "ZL", "ZU", "ZW", "ZP", "ZH", "ZZ", "VM", "VH", "RK",
    ];
    let allowed: HashSet<&str> = allowed.into_iter().collect();
    let mut out = Vec::with_capacity(required_identifiers.len());
    let mut reader = open_dat_reader(file_path)?;
    let mut line = String::new();

    if required_identifiers.is_empty() {
        return Ok(out);
    }

    loop {
        line.clear();
        if reader.read_line(&mut line)? == 0 {
            break;
        }

        let mut cursor = 0usize;
        let Some(_lat) = next_ascii_field(&line, &mut cursor) else {
            continue;
        };
        let Some(_lon) = next_ascii_field(&line, &mut cursor) else {
            continue;
        };
        let Some(identifier) = next_ascii_field(&line, &mut cursor) else {
            continue;
        };
        if !required_identifiers.contains(identifier) {
            continue;
        }
        let Some(scope) = next_ascii_field(&line, &mut cursor) else {
            continue;
        };
        let Some(icao_code) = next_ascii_field(&line, &mut cursor) else {
            continue;
        };

        if scope == "ENRT" && allowed.contains(icao_code) {
            out.push((identifier.to_string(), icao_code.to_string()));
        }
    }

    Ok(out)
}

fn load_earth_nav_items(
    file_path: &str,
    required_vor_dme_identifiers: &HashSet<Box<str>>,
    required_ndb_identifiers: &HashSet<Box<str>>,
) -> Result<Vec<(String, String, String)>> {
    let allowed = [
        "ZB", "ZS", "ZJ", "ZG", "ZY", "ZL", "ZU", "ZW", "ZP", "ZH", "ZZ", "VM", "VH", "RK",
    ];
    let allowed: HashSet<&str> = allowed.into_iter().collect();
    let mut out =
        Vec::with_capacity(required_vor_dme_identifiers.len() + required_ndb_identifiers.len());
    let mut reader = open_dat_reader(file_path)?;
    let mut line = String::new();

    if required_vor_dme_identifiers.is_empty() && required_ndb_identifiers.is_empty() {
        return Ok(out);
    }

    loop {
        line.clear();
        if reader.read_line(&mut line)? == 0 {
            break;
        }

        let mut cursor = 0usize;
        let Some(_record_type) = next_ascii_field(&line, &mut cursor) else {
            continue;
        };
        let Some(_lat) = next_ascii_field(&line, &mut cursor) else {
            continue;
        };
        let Some(_lon) = next_ascii_field(&line, &mut cursor) else {
            continue;
        };
        let Some(_) = next_ascii_field(&line, &mut cursor) else {
            continue;
        };
        let Some(_) = next_ascii_field(&line, &mut cursor) else {
            continue;
        };
        let Some(_) = next_ascii_field(&line, &mut cursor) else {
            continue;
        };
        let Some(_identifier_6) = next_ascii_field(&line, &mut cursor) else {
            continue;
        };
        let Some(identifier) = next_ascii_field(&line, &mut cursor) else {
            continue;
        };
        if !required_vor_dme_identifiers.contains(identifier)
            && !required_ndb_identifiers.contains(identifier)
        {
            continue;
        }
        let Some(scope) = next_ascii_field(&line, &mut cursor) else {
            continue;
        };
        let Some(icao_code) = next_ascii_field(&line, &mut cursor) else {
            continue;
        };

        let mut last_field = None;
        while let Some(field) = next_ascii_field(&line, &mut cursor) {
            last_field = Some(field);
        }

        if scope == "ENRT" && allowed.contains(icao_code) {
            if let Some(nav_type) = last_field {
                match nav_type {
                    "VOR/DME" if required_vor_dme_identifiers.contains(identifier) => {
                        out.push((
                            identifier.to_string(),
                            nav_type.to_string(),
                            icao_code.to_string(),
                        ));
                    }
                    "NDB" if required_ndb_identifiers.contains(identifier) => {
                        out.push((
                            identifier.to_string(),
                            nav_type.to_string(),
                            icao_code.to_string(),
                        ));
                    }
                    _ => {}
                }
            }
        }
    }

    Ok(out)
}

fn parse_airway_csv_rows(csv_file: &str) -> Result<Vec<ParsedAirwayCsvRow>> {
    parse_airway_csv_rows_simple(csv_file)
}

fn get_scoped_airways_icao_resolver(
    earth_fix_file: &str,
    earth_nav_file: &str,
    identifiers: &ResolverIdentifierSets,
) -> Result<IcaoCodeResolver> {
    let fix_items = load_earth_fix_items(earth_fix_file, &identifiers.fix_identifiers)?;
    let nav_items = load_earth_nav_items(
        earth_nav_file,
        &identifiers.vor_dme_identifiers,
        &identifiers.ndb_identifiers,
    )?;
    Ok(IcaoCodeResolver::from_items(fix_items, nav_items))
}

fn start_code_mapping_from_type(
    code_type: AirwayCodeType,
) -> Option<(&'static str, &'static str, &'static str)> {
    match code_type {
        AirwayCodeType::DesignatedPoint => Some(("fix", "E C", "tbl_enroute_waypoints")),
        AirwayCodeType::VorDme => Some(("vhf", "V C", "tbl_vhfnavaids")),
        AirwayCodeType::Ndb => Some(("ndb", "E C", "tbl_enroute_ndbnavaids")),
        AirwayCodeType::Other => None,
    }
}

fn normalize_direction_restriction(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("X") {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn reverse_direction_restriction(value: &str) -> String {
    if value.eq_ignore_ascii_case("F") {
        "B".to_string()
    } else if value.eq_ignore_ascii_case("B") {
        "F".to_string()
    } else {
        value.to_string()
    }
}

fn build_airway_direction_map(rows: &[ParsedAirwayCsvRow]) -> AirwayDirectionRouteMap {
    let mut out = AirwayDirectionRouteMap::new();

    for row in rows {
        let Some(direction) = row
            .direction_restriction
            .as_deref()
            .and_then(normalize_direction_restriction)
        else {
            continue;
        };
        let Some(end_waypoint) = row.end_waypoint.as_deref().map(str::trim) else {
            continue;
        };
        if end_waypoint.is_empty() || row.start_waypoint.trim().is_empty() {
            continue;
        }

        out.entry(row.route_identifier.to_string())
            .or_default()
            .entry(row.start_waypoint.trim().to_string())
            .or_default()
            .insert(end_waypoint.to_string(), direction);
    }

    out
}

fn resolve_airway_direction_restriction(
    direction_map: &AirwayDirectionRouteMap,
    route_identifier: &str,
    current_waypoint: &str,
    next_waypoint: &str,
) -> Option<String> {
    let route_map = direction_map.get(route_identifier)?;

    if let Some(direction) = route_map
        .get(current_waypoint)
        .and_then(|endpoint_map| endpoint_map.get(next_waypoint))
    {
        return Some(direction.clone());
    }

    route_map
        .get(next_waypoint)
        .and_then(|endpoint_map| endpoint_map.get(current_waypoint))
        .map(|direction| reverse_direction_restriction(direction))
}

fn apply_airway_direction_restrictions(rows: &mut [AirwayRecord], csv_rows: &[ParsedAirwayCsvRow]) {
    if rows.is_empty() {
        return;
    }

    let direction_map = build_airway_direction_map(csv_rows);

    let mut route_start = 0usize;
    while route_start < rows.len() {
        let route_identifier = rows[route_start].route_identifier.clone();
        let mut route_end = route_start + 1;
        while route_end < rows.len() && rows[route_end].route_identifier == route_identifier {
            route_end += 1;
        }

        for current_idx in route_start..route_end {
            let direction = if current_idx + 1 < route_end {
                resolve_airway_direction_restriction(
                    &direction_map,
                    route_identifier.as_str(),
                    rows[current_idx].waypoint_identifier.as_str(),
                    rows[current_idx + 1].waypoint_identifier.as_str(),
                )
            } else if current_idx > route_start {
                resolve_airway_direction_restriction(
                    &direction_map,
                    route_identifier.as_str(),
                    rows[current_idx - 1].waypoint_identifier.as_str(),
                    rows[current_idx].waypoint_identifier.as_str(),
                )
            } else {
                None
            };
            rows[current_idx].direction_restriction = direction;
        }

        route_start = route_end;
    }
}

fn prefetch_airway_coordinates(
    conn: &RustSqliteConnection,
    rows: &[ParsedAirwayCsvRow],
) -> Result<AirwayCoordCache> {
    let mut identifiers_by_type: HashMap<AirwayAidType, HashSet<Box<str>>> = HashMap::new();
    for row in rows {
        if let Some((aid_type, _, _)) = start_code_mapping_from_type(row.start_code_type) {
            if !row.start_waypoint.is_empty() {
                identifiers_by_type
                    .entry(aid_type)
                    .or_default()
                    .insert(row.start_waypoint.clone());
            }
        }

        let Some(end_waypoint) = row.end_waypoint.as_ref().filter(|value| !value.is_empty()) else {
            continue;
        };
        let Some(end_code_type) = row.end_code_type else {
            continue;
        };
        let Some((aid_type, _, _)) = start_code_mapping_from_type(end_code_type) else {
            continue;
        };
        identifiers_by_type
            .entry(aid_type)
            .or_default()
            .insert(end_waypoint.clone());
    }

    let mut cache = AirwayCoordCache::default();

    for (aid_type, identifiers) in identifiers_by_type {
        if identifiers.is_empty() {
            continue;
        }

        let Some(cache_map) = cache.map_mut(aid_type) else {
            continue;
        };

        let query_template = match aid_type {
            "fix" => {
                "SELECT waypoint_identifier, waypoint_latitude, waypoint_longitude FROM tbl_enroute_waypoints WHERE waypoint_identifier IN ({placeholders}) AND icao_code IN ('ZW','ZG','ZS','ZY','ZL','ZH','ZU','ZP','ZB','ZJ','ZZ','VM','VH') ORDER BY rowid"
            }
            "vhf" => {
                "SELECT vor_identifier, vor_latitude, vor_longitude FROM tbl_vhfnavaids WHERE vor_identifier IN ({placeholders}) AND icao_code IN ('ZW','ZG','ZS','ZY','ZL','ZH','ZU','ZP','ZB','ZJ','ZZ','VM','VH') ORDER BY rowid"
            }
            "ndb" => {
                "SELECT ndb_identifier, ndb_latitude, ndb_longitude FROM tbl_enroute_ndbnavaids WHERE ndb_identifier IN ({placeholders}) AND icao_code IN ('ZW','ZG','ZS','ZY','ZL','ZH','ZU','ZP','ZB','ZJ','ZZ','VM','VH') ORDER BY rowid"
            }
            _ => continue,
        };

        let identifiers_list: Vec<String> = identifiers.into_iter().map(String::from).collect();
        for chunk in identifiers_list.chunks(500) {
            let placeholders = vec!["?"; chunk.len()].join(",");
            let query = query_template.replace("{placeholders}", &placeholders);
            let params = chunk
                .iter()
                .map(|identifier| SqlValue::Text(identifier.clone()))
                .collect::<Vec<_>>();
            conn.query_each_native(&query, &params, |row| {
                let identifier: String = row.get(0)?;
                let lat = row.get::<_, Option<f64>>(1)?;
                let lon = row.get::<_, Option<f64>>(2)?;
                cache_map.entry(identifier).or_insert_with(|| lat.zip(lon));
                Ok(())
            })
            .map_err(sqlite_error)?;
        }

        for identifier in identifiers_list {
            cache_map.entry(identifier).or_insert(None);
        }
    }

    Ok(cache)
}

fn airway_record_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<AirwayRecord> {
    Ok(AirwayRecord {
        area_code: row
            .get::<_, Option<String>>(0)?
            .unwrap_or_else(|| "EEU".to_string()),
        route_identifier: row.get::<_, Option<String>>(1)?.unwrap_or_default(),
        seqno: row.get::<_, Option<i64>>(2)?.unwrap_or(1000),
        icao_code: row.get(3)?,
        waypoint_identifier: row.get::<_, Option<String>>(4)?.unwrap_or_default(),
        waypoint_latitude: row.get(5)?,
        waypoint_longitude: row.get(6)?,
        waypoint_description_code: row.get::<_, Option<String>>(7)?.unwrap_or_default(),
        route_type: row
            .get::<_, Option<String>>(8)?
            .unwrap_or_else(|| "O".to_string()),
        flightlevel: row
            .get::<_, Option<String>>(9)?
            .unwrap_or_else(|| "B".to_string()),
        direction_restriction: row.get(10)?,
        crusing_table_identifier: row.get(11)?,
        minimum_altitude1: row.get(12)?,
        minimum_altitude2: row.get(13)?,
        maximum_altitude: row.get(14)?,
        outbound_course: row.get::<_, Option<f64>>(15)?.unwrap_or(0.0),
        inbound_course: row.get::<_, Option<f64>>(16)?.unwrap_or(0.0),
        inbound_distance: row.get::<_, Option<f64>>(17)?.unwrap_or(0.0),
        id: row
            .get::<_, Option<String>>(18)?
            .filter(|value| !value.trim().is_empty()),
    })
}

fn build_airway_reference_id(
    ref_table: &str,
    icao_code: Option<&str>,
    waypoint_identifier: &str,
) -> Option<String> {
    let icao_code = icao_code.map(str::trim).filter(|value| !value.is_empty())?;
    let waypoint_identifier = waypoint_identifier.trim();
    if waypoint_identifier.is_empty() {
        return None;
    }

    let raw_id = match ref_table {
        "tbl_enroute_waypoints" | "tbl_vhfnavaids" | "tbl_enroute_ndbnavaids" => {
            format!("{}{}", icao_code, waypoint_identifier)
        }
        _ => return None,
    };

    Some(format!("{}|{}", ref_table, raw_id))
}

fn build_processed_airway_rows(
    rows: &[ParsedAirwayCsvRow],
    coord_cache: &AirwayCoordCache,
    icao_resolver: &IcaoCodeResolver,
) -> Vec<AirwayRecord> {
    let mut out = Vec::with_capacity(rows.len());
    let mut previous_outbound_by_route: HashMap<String, f64> = HashMap::new();

    for row in rows {
        let icao_code = icao_resolver.resolve_ref(
            Some(row.start_waypoint.as_ref()),
            row.start_code_type.resolver_code(),
        );
        let inbound_course = previous_outbound_by_route
            .get(row.route_identifier.as_ref())
            .copied()
            .unwrap_or(0.0);
        previous_outbound_by_route.insert(row.route_identifier.to_string(), row.outbound_course);

        let (waypoint_description_code, waypoint_ref_table, waypoint_latitude, waypoint_longitude) =
            if let Some((aid_type, description_code, ref_table)) =
                start_code_mapping_from_type(row.start_code_type)
            {
                let coords = coord_cache.get(aid_type, row.start_waypoint.as_ref());
                (
                    description_code.to_string(),
                    ref_table.to_string(),
                    coords.map(|value| value.0),
                    coords.map(|value| value.1),
                )
            } else {
                (String::new(), String::new(), None, None)
            };

        out.push(AirwayRecord {
            area_code: "EEU".to_string(),
            crusing_table_identifier: Some("EE".to_string()),
            direction_restriction: None,
            flightlevel: "B".to_string(),
            icao_code: icao_code.clone(),
            inbound_course,
            inbound_distance: row.inbound_distance,
            maximum_altitude: Some(99999),
            minimum_altitude1: Some(5000),
            minimum_altitude2: None,
            outbound_course: row.outbound_course,
            route_identifier: row.route_identifier.to_string(),
            route_type: "O".to_string(),
            seqno: 0,
            waypoint_description_code,
            waypoint_identifier: row.start_waypoint.to_string(),
            waypoint_latitude,
            waypoint_longitude,
            id: build_airway_reference_id(
                waypoint_ref_table.as_str(),
                icao_code.as_deref(),
                row.start_waypoint.as_ref(),
            ),
        });
    }

    out
}

fn fetch_existing_airway_rows(
    conn: &RustSqliteConnection,
    routes: &[String],
) -> Result<Vec<AirwayRecord>> {
    let mut out = Vec::new();
    for chunk in routes.chunks(500) {
        let placeholders = vec!["?"; chunk.len()].join(",");
        let query = format!(
            "SELECT area_code, route_identifier, seqno, icao_code, waypoint_identifier, waypoint_latitude, waypoint_longitude, waypoint_description_code, route_type, flightlevel, direction_restriction, crusing_table_identifier, minimum_altitude1, minimum_altitude2, maximum_altitude, outbound_course, inbound_course, inbound_distance, id FROM {} WHERE route_identifier IN ({}) ORDER BY route_identifier, seqno",
            ENROUTE_AIRWAYS_TABLE,
            placeholders
        );
        let params = chunk
            .iter()
            .map(|route| SqlValue::Text(route.clone()))
            .collect::<Vec<_>>();
        conn.query_each_native(&query, &params, |row| {
            out.push(airway_record_from_row(row)?);
            Ok(())
        })
        .map_err(sqlite_error)?;
    }
    Ok(out)
}

fn append_airway_end_rows(
    final_rows: &mut Vec<AirwayRecord>,
    csv_rows: &[ParsedAirwayCsvRow],
    route_order: &[String],
    coord_cache: &AirwayCoordCache,
    icao_resolver: &IcaoCodeResolver,
) {
    let existing_route_wps: Vec<ExistingRouteWaypoint> = final_rows
        .iter()
        .map(|row| {
            (
                row.route_identifier.clone(),
                row.waypoint_identifier.clone(),
            )
        })
        .collect();
    let mut max_seq_map: HashMap<String, i64> = HashMap::new();
    for row in final_rows.iter() {
        max_seq_map
            .entry(row.route_identifier.clone())
            .and_modify(|seq| *seq = (*seq).max(row.seqno))
            .or_insert(row.seqno);
    }
    let max_seq_items: Vec<MaxSeqItem> = max_seq_map.into_iter().collect();

    let mut last_rows_by_route: HashMap<String, &ParsedAirwayCsvRow> = HashMap::new();
    for row in csv_rows {
        last_rows_by_route.insert(row.route_identifier.to_string(), row);
    }

    let mut payload: Vec<AirwayEndCsvLastRow> = Vec::new();
    for route in route_order {
        let Some(row) = last_rows_by_route.get(route) else {
            continue;
        };
        payload.push((
            row.route_identifier.to_string(),
            row.end_waypoint.as_deref().map(str::to_string),
            Some(row.outbound_course),
            row.end_code_type,
        ));
    }

    let end_plan = build_airway_end_append_plan(payload, existing_route_wps, max_seq_items);
    for (
        route,
        end_wp,
        inbound_course,
        end_code_type,
        end_description_code,
        end_ref_table,
        seqno,
    ) in end_plan
    {
        let icao_code = icao_resolver.resolve_ref(
            Some(end_wp.as_str()),
            end_code_type.and_then(AirwayCodeType::resolver_code),
        );
        let coords = end_code_type
            .and_then(start_code_mapping_from_type)
            .and_then(|(aid_type, _, _)| coord_cache.get(aid_type, end_wp.as_str()));
        let id = build_airway_reference_id(end_ref_table.as_str(), icao_code.as_deref(), &end_wp);
        final_rows.push(AirwayRecord {
            area_code: "EEU".to_string(),
            crusing_table_identifier: Some("EE".to_string()),
            direction_restriction: None,
            flightlevel: "B".to_string(),
            icao_code: icao_code.clone(),
            inbound_course,
            inbound_distance: 0.0,
            maximum_altitude: Some(99999),
            minimum_altitude1: Some(5000),
            minimum_altitude2: None,
            outbound_course: 0.0,
            route_identifier: route,
            route_type: "O".to_string(),
            seqno,
            waypoint_description_code: end_description_code,
            waypoint_identifier: end_wp,
            waypoint_latitude: coords.map(|value| value.0),
            waypoint_longitude: coords.map(|value| value.1),
            id,
        });
    }
}

fn bind_airway_row(stmt: &mut rusqlite::Statement<'_>, row: &AirwayRecord) -> rusqlite::Result<()> {
    stmt.raw_bind_parameter(1, row.area_code.as_str())?;
    stmt.raw_bind_parameter(2, row.route_identifier.as_str())?;
    stmt.raw_bind_parameter(3, row.seqno)?;
    match &row.icao_code {
        Some(v) => stmt.raw_bind_parameter(4, v.as_str())?,
        None => stmt.raw_bind_parameter(4, rusqlite::types::Null)?,
    }
    stmt.raw_bind_parameter(5, row.waypoint_identifier.as_str())?;
    match row.waypoint_latitude {
        Some(v) => stmt.raw_bind_parameter(6, v)?,
        None => stmt.raw_bind_parameter(6, rusqlite::types::Null)?,
    }
    match row.waypoint_longitude {
        Some(v) => stmt.raw_bind_parameter(7, v)?,
        None => stmt.raw_bind_parameter(7, rusqlite::types::Null)?,
    }
    stmt.raw_bind_parameter(8, row.waypoint_description_code.as_str())?;
    stmt.raw_bind_parameter(9, row.route_type.as_str())?;
    stmt.raw_bind_parameter(10, row.flightlevel.as_str())?;
    match &row.direction_restriction {
        Some(v) => stmt.raw_bind_parameter(11, v.as_str())?,
        None => stmt.raw_bind_parameter(11, rusqlite::types::Null)?,
    }
    match &row.crusing_table_identifier {
        Some(v) => stmt.raw_bind_parameter(12, v.as_str())?,
        None => stmt.raw_bind_parameter(12, rusqlite::types::Null)?,
    }
    match row.minimum_altitude1 {
        Some(v) => stmt.raw_bind_parameter(13, v)?,
        None => stmt.raw_bind_parameter(13, rusqlite::types::Null)?,
    }
    match row.minimum_altitude2 {
        Some(v) => stmt.raw_bind_parameter(14, v)?,
        None => stmt.raw_bind_parameter(14, rusqlite::types::Null)?,
    }
    match row.maximum_altitude {
        Some(v) => stmt.raw_bind_parameter(15, v)?,
        None => stmt.raw_bind_parameter(15, rusqlite::types::Null)?,
    }
    stmt.raw_bind_parameter(16, row.outbound_course)?;
    stmt.raw_bind_parameter(17, row.inbound_course)?;
    stmt.raw_bind_parameter(18, row.inbound_distance)?;
    match row.id.as_deref().filter(|value| !value.trim().is_empty()) {
        Some(value) => stmt.raw_bind_parameter(19, value)?,
        None => stmt.raw_bind_parameter(19, rusqlite::types::Null)?,
    }
    stmt.raw_execute()?;
    Ok(())
}

fn insert_airway_rows(
    conn: &RustSqliteConnection,
    rows: &[AirwayRecord],
    batch_size: usize,
) -> Result<()> {
    let insert_sql = "INSERT INTO tbl_enroute_airways (area_code, route_identifier, seqno, icao_code, waypoint_identifier, waypoint_latitude, waypoint_longitude, waypoint_description_code, route_type, flightlevel, direction_restriction, crusing_table_identifier, minimum_altitude1, minimum_altitude2, maximum_altitude, outbound_course, inbound_course, inbound_distance, id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    for chunk in rows.chunks(batch_size.max(1)) {
        conn.with_connection_native(|raw_conn| {
            let tx = raw_conn.unchecked_transaction()?;
            {
                let mut stmt = tx.prepare(&insert_sql)?;
                for row in chunk {
                    bind_airway_row(&mut stmt, row)?;
                }
            }
            tx.commit()?;
            Ok(())
        })?;
    }
    Ok(())
}

fn apply_airway_segment_metrics(
    rows: &mut [AirwayRecord],
    routes_with_missing: &[String],
) -> Result<()> {
    if routes_with_missing.is_empty() || rows.is_empty() {
        return Ok(());
    }

    let affected_routes: HashSet<&str> = routes_with_missing.iter().map(String::as_str).collect();
    let mut tasks: Vec<AirwaySegmentTask> = Vec::new();
    let mut route_start = 0usize;

    while route_start < rows.len() {
        let route_identifier = rows[route_start].route_identifier.as_str();
        let mut route_end = route_start + 1;
        while route_end < rows.len() && rows[route_end].route_identifier == route_identifier {
            route_end += 1;
        }

        if affected_routes.contains(route_identifier) {
            if route_end - route_start < 2 {
                rows[route_start].inbound_distance = 0.0;
                rows[route_start].outbound_course = 0.0;
            } else {
                for current_idx in route_start..route_end - 1 {
                    let next_idx = current_idx + 1;
                    tasks.push((
                        current_idx,
                        next_idx,
                        rows[current_idx].waypoint_latitude,
                        rows[current_idx].waypoint_longitude,
                        rows[next_idx].waypoint_latitude,
                        rows[next_idx].waypoint_longitude,
                    ));
                }
                rows[route_end - 1].inbound_distance = 0.0;
                rows[route_end - 1].outbound_course = 0.0;
            }
        }

        route_start = route_end;
    }

    if tasks.is_empty() {
        return Ok(());
    }

    let mut declinations = vec![0.0; tasks.len()];
    let mut coord_indices = Vec::new();
    let mut coords = Vec::new();

    for (idx, (_, _, curr_lat, curr_lon, next_lat, next_lon)) in tasks.iter().enumerate() {
        if let (Some(curr_lat), Some(curr_lon), Some(_), Some(_)) =
            (curr_lat, curr_lon, next_lat, next_lon)
        {
            coords.push((*curr_lat, *curr_lon));
            coord_indices.push(idx);
        }
    }
    if !coords.is_empty() {
        let values = batch_get_magnetic_variations_internal(&coords)?;
        for (idx, declination) in coord_indices.into_iter().zip(values) {
            declinations[idx] = declination;
        }
    }

    for (current_idx, next_idx, inbound_distance, outbound_course) in
        build_airway_segment_metrics(tasks, declinations)?
    {
        rows[current_idx].inbound_distance = inbound_distance;
        rows[current_idx].outbound_course = outbound_course;
        rows[next_idx].inbound_course = outbound_course;
    }

    Ok(())
}

pub(crate) fn process_airways_to_db(
    csv_file: &str,
    earth_fix_file: &str,
    earth_nav_file: &str,
    conn: &RustSqliteConnection,
) -> Result<(usize, usize)> {
    let csv_rows = parse_airway_csv_rows(csv_file)
        .map_err(|err| anyhow!("parse_airway_csv_rows failed: {}", err))?;

    conn.execute_statement_native("CREATE TABLE IF NOT EXISTS tbl_enroute_airways (area_code TEXT(3), route_identifier TEXT(6), seqno INTEGER(4), icao_code TEXT(2), waypoint_identifier TEXT(5), waypoint_latitude DOUBLE(9), waypoint_longitude DOUBLE(10), waypoint_description_code TEXT(4), route_type TEXT(1), flightlevel TEXT(1), direction_restriction TEXT(1), crusing_table_identifier TEXT(2), minimum_altitude1 INTEGER(5), minimum_altitude2 INTEGER(5), maximum_altitude INTEGER(5), outbound_course DOUBLE(5), inbound_course DOUBLE(5), inbound_distance DOUBLE(5), id TEXT(15))", &[]).map_err(sqlite_error)?;
    conn.execute_statement_native("CREATE INDEX IF NOT EXISTS idx_enroute_airways_route_seq ON tbl_enroute_airways(route_identifier, seqno)", &[]).map_err(sqlite_error)?;

    if csv_rows.is_empty() {
        return Ok((0, 0));
    }

    let resolver_identifiers = collect_airways_resolver_identifiers(&csv_rows);

    let icao_resolver =
        get_scoped_airways_icao_resolver(earth_fix_file, earth_nav_file, &resolver_identifiers)
            .map_err(|err| anyhow!("get_scoped_airways_icao_resolver failed: {}", err))?;

    let coord_cache = prefetch_airway_coordinates(conn, &csv_rows)
        .map_err(|err| anyhow!("prefetch_airway_coordinates failed: {}", err))?;

    let processed_rows = build_processed_airway_rows(&csv_rows, &coord_cache, &icao_resolver);

    let mut unique_routes = Vec::new();
    let mut seen_routes = HashSet::new();
    for row in &processed_rows {
        if seen_routes.insert(row.route_identifier.clone()) {
            unique_routes.push(row.route_identifier.clone());
        }
    }

    let db_rows_all = fetch_existing_airway_rows(conn, &unique_routes)
        .map_err(|err| anyhow!("fetch_existing_airway_rows failed: {}", err))?;

    let mut csv_route_map: CsvRouteMap = HashMap::new();
    for (idx, record) in processed_rows.iter().enumerate() {
        csv_route_map
            .entry(record.route_identifier.clone())
            .or_default()
            .push((record.waypoint_identifier.clone(), idx));
    }

    let mut db_route_map: DbRouteMap = HashMap::new();
    for (idx, record) in db_rows_all.iter().enumerate() {
        db_route_map
            .entry(record.route_identifier.clone())
            .or_default()
            .push((record.waypoint_identifier.clone(), idx, record.seqno));
    }

    let (row_plan, routes_to_delete, routes_with_missing) =
        build_airway_merge_plan(&unique_routes, &csv_route_map, &db_route_map);

    let mut merged_rows_all = Vec::with_capacity(row_plan.len());
    for (source, row_idx, seq_override) in row_plan {
        let mut row = match source {
            RowSource::Csv => processed_rows[row_idx].clone(),
            RowSource::Db => db_rows_all[row_idx].clone(),
        };
        if let Some(seqno) = seq_override {
            row.seqno = seqno;
        }
        merged_rows_all.push(row);
    }

    for chunk in routes_to_delete.chunks(500) {
        let placeholders = vec!["?"; chunk.len()].join(",");
        let query = format!(
            "DELETE FROM {} WHERE route_identifier IN ({})",
            ENROUTE_AIRWAYS_TABLE, placeholders
        );
        let params = chunk
            .iter()
            .map(|route| SqlValue::Text(route.clone()))
            .collect::<Vec<_>>();
        conn.execute_statement_native(&query, &params)
            .map_err(sqlite_error)?;
    }

    let mut final_rows = if merged_rows_all.is_empty() {
        processed_rows
    } else {
        merged_rows_all
    };

    append_airway_end_rows(
        &mut final_rows,
        &csv_rows,
        &unique_routes,
        &coord_cache,
        &icao_resolver,
    );

    final_rows.sort_by(|left, right| {
        left.route_identifier
            .cmp(&right.route_identifier)
            .then(left.seqno.cmp(&right.seqno))
    });

    apply_airway_direction_restrictions(&mut final_rows, &csv_rows);

    apply_airway_segment_metrics(&mut final_rows, &routes_with_missing)
        .map_err(|err| anyhow!("apply_airway_segment_metrics failed: {}", err))?;

    insert_airway_rows(conn, &final_rows, 2000)
        .map_err(|err| anyhow!("insert_airway_rows failed: {}", err))?;

    Ok((final_rows.len(), routes_with_missing.len()))
}

fn sqlite_error(err: rusqlite::Error) -> anyhow::Error {
    anyhow!(err.to_string())
}

pub(crate) fn parse_dms_list(values: Vec<Option<String>>) -> Vec<Option<f64>> {
    values
        .into_iter()
        .map(|v| v.as_deref().and_then(parse_dms_to_decimal))
        .collect()
}

fn build_airway_end_append_plan(
    csv_last_rows: Vec<AirwayEndCsvLastRow>,
    existing_route_wps: Vec<ExistingRouteWaypoint>,
    max_seq_items: Vec<MaxSeqItem>,
) -> Vec<AirwayEndAppendPlanRow> {
    let existing_set: HashSet<ExistingRouteWaypoint> = existing_route_wps.into_iter().collect();
    let mut max_seq_map: HashMap<String, i64> = max_seq_items.into_iter().collect();

    let mut out = Vec::new();

    for (route, end_wp_opt, inbound_opt, end_code_type) in csv_last_rows {
        let Some(end_wp_raw) = end_wp_opt else {
            continue;
        };
        let end_wp = end_wp_raw.trim().to_string();
        if end_wp.is_empty() {
            continue;
        }
        if existing_set.contains(&(route.clone(), end_wp.clone())) {
            continue;
        }

        let inbound_course = inbound_opt.unwrap_or(0.0);

        let (end_description_code, end_ref_table) =
            if end_code_type == Some(AirwayCodeType::DesignatedPoint) {
                ("EEC".to_string(), "tbl_enroute_waypoints".to_string())
            } else if end_code_type == Some(AirwayCodeType::VorDme) {
                ("VEC".to_string(), "tbl_vhfnavaids".to_string())
            } else if end_code_type == Some(AirwayCodeType::Ndb) {
                ("EEC".to_string(), "tbl_enroute_ndbnavaids".to_string())
            } else {
                ("".to_string(), "".to_string())
            };

        let current_max_seq = *max_seq_map.get(&route).unwrap_or(&1000);
        let new_seqno = current_max_seq + 5;
        max_seq_map.insert(route.clone(), new_seqno);

        out.push((
            route,
            end_wp,
            inbound_course,
            end_code_type,
            end_description_code,
            end_ref_table,
            new_seqno,
        ));
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn write_temp_test_file(prefix: &str, contents: &str) -> String {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("{}_{}.dat", prefix, unique));
        fs::write(&path, contents).unwrap();
        path.to_string_lossy().into_owned()
    }

    fn test_airway_record(route: &str, waypoint: &str, seqno: i64) -> AirwayRecord {
        AirwayRecord {
            area_code: "EEU".to_string(),
            crusing_table_identifier: Some("EE".to_string()),
            direction_restriction: Some("STALE".to_string()),
            flightlevel: "B".to_string(),
            icao_code: None,
            inbound_course: 0.0,
            inbound_distance: 0.0,
            maximum_altitude: Some(99999),
            minimum_altitude1: Some(5000),
            minimum_altitude2: None,
            outbound_course: 0.0,
            route_identifier: route.to_string(),
            route_type: "O".to_string(),
            seqno,
            waypoint_description_code: String::new(),
            waypoint_identifier: waypoint.to_string(),
            waypoint_latitude: None,
            waypoint_longitude: None,
            id: None,
        }
    }

    fn test_csv_row(
        route: &str,
        start: &str,
        direction: Option<&str>,
        end: Option<&str>,
    ) -> ParsedAirwayCsvRow {
        ParsedAirwayCsvRow {
            route_identifier: route.into(),
            start_waypoint: start.into(),
            start_code_type: AirwayCodeType::DesignatedPoint,
            direction_restriction: direction.map(Into::into),
            outbound_course: 0.0,
            inbound_distance: 0.0,
            end_waypoint: end.map(Into::into),
            end_code_type: Some(AirwayCodeType::DesignatedPoint),
        }
    }

    #[test]
    fn airway_segment_metrics_default_when_coordinates_missing() {
        let tasks = vec![
            (0, 1, Some(30.0), Some(120.0), Some(31.0), Some(121.0)),
            (2, 3, None, Some(120.0), Some(31.0), Some(121.0)),
            (4, 5, Some(30.0), Some(120.0), Some(31.0), None),
        ];
        let declinations = vec![0.0, 0.0, 0.0];

        let metrics = build_airway_segment_metrics(tasks, declinations).unwrap();

        assert_eq!(metrics.len(), 3);

        assert_eq!(metrics[0].0, 0);
        assert_eq!(metrics[0].1, 1);
        assert!(metrics[0].2 > 0.0);
        assert!((0.0..=360.0).contains(&metrics[0].3));

        assert_eq!(metrics[1].2, 0.0);
        assert_eq!(metrics[1].3, 0.0);

        assert_eq!(metrics[2].2, 0.0);
        assert_eq!(metrics[2].3, 0.0);
    }

    #[test]
    fn airway_segment_metrics_reject_length_mismatch() {
        let tasks = vec![(0, 1, Some(30.0), Some(120.0), Some(31.0), Some(121.0))];
        let declinations = vec![];

        let result = build_airway_segment_metrics(tasks, declinations);
        assert!(result.is_err());
    }

    #[test]
    fn parse_airway_csv_rows_reads_code_dir_column() {
        let csv = "\u{feff}TXT_DESIG,CODE_POINT_START,CODE_TYPE_START,CODE_DIR,CODE_POINT_END,CODE_TYPE_END,VAL_MAG_TRACK,VAL_LEN\r\nA1,AAA,DESIGNATED_POINT,F,BBB,DESIGNATED_POINT,123,10\r\n";
        let rows = parse_airway_csv_rows_simple_from_bufread(Cursor::new(csv)).unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].direction_restriction.as_deref(), Some("F"));
    }

    #[test]
    fn airway_direction_restrictions_follow_final_route_order() {
        let csv_rows = vec![
            test_csv_row("A1", "AAA", Some("F"), Some("BBB")),
            test_csv_row("A1", "BBB", Some("X"), Some("CCC")),
            test_csv_row("B1", "DDD", Some("B"), Some("EEE")),
            test_csv_row("D1", "HHH", Some("F"), Some("III")),
            test_csv_row("C1", "FFF", Some("F"), Some("GGG")),
        ];

        let mut rows = vec![
            test_airway_record("A1", "AAA", 1000),
            test_airway_record("A1", "BBB", 1005),
            test_airway_record("A1", "CCC", 1010),
            test_airway_record("B1", "EEE", 1000),
            test_airway_record("B1", "DDD", 1005),
            test_airway_record("D1", "HHH", 1000),
            test_airway_record("D1", "III", 1005),
            test_airway_record("C1", "FFF", 1000),
            test_airway_record("C1", "XXX", 1005),
            test_airway_record("C1", "GGG", 1010),
        ];

        apply_airway_direction_restrictions(&mut rows, &csv_rows);

        assert_eq!(rows[0].direction_restriction.as_deref(), Some("F"));
        assert_eq!(rows[1].direction_restriction, None);
        assert_eq!(rows[2].direction_restriction, None);

        assert_eq!(rows[3].direction_restriction.as_deref(), Some("F"));
        assert_eq!(rows[4].direction_restriction.as_deref(), Some("F"));

        assert_eq!(rows[5].direction_restriction.as_deref(), Some("F"));
        assert_eq!(rows[6].direction_restriction.as_deref(), Some("F"));

        assert_eq!(rows[7].direction_restriction, None);
        assert_eq!(rows[8].direction_restriction, None);
        assert_eq!(rows[9].direction_restriction, None);
    }

    #[test]
    fn load_earth_fix_items_keeps_zz_records() {
        let path = write_temp_test_file(
            "airway_fix_zz",
            "31.0 121.0 FIXZZ ENRT ZZ\n31.0 121.0 FIXZS ENRT ZS\n",
        );
        let required = HashSet::from([Box::<str>::from("FIXZZ"), Box::<str>::from("FIXZS")]);

        let rows = load_earth_fix_items(&path, &required).unwrap();
        fs::remove_file(path).unwrap();

        assert!(rows
            .iter()
            .any(|(identifier, icao)| identifier == "FIXZZ" && icao == "ZZ"));
        assert!(rows
            .iter()
            .any(|(identifier, icao)| identifier == "FIXZS" && icao == "ZS"));
    }

    #[test]
    fn load_earth_nav_items_keeps_zz_records() {
        let path = write_temp_test_file(
            "airway_nav_zz",
            "12 31.0 121.0 10 11320 50 X NAVZZ ENRT ZZ NAVNAME VOR/DME\n2 30.0 120.0 0 375 25 X NDBZZ ENRT ZZ NDBNAME NDB\n",
        );
        let vor_required = HashSet::from([Box::<str>::from("NAVZZ")]);
        let ndb_required = HashSet::from([Box::<str>::from("NDBZZ")]);

        let rows = load_earth_nav_items(&path, &vor_required, &ndb_required).unwrap();
        fs::remove_file(path).unwrap();

        assert!(rows.iter().any(|(identifier, nav_type, icao)| {
            identifier == "NAVZZ" && nav_type == "VOR/DME" && icao == "ZZ"
        }));
        assert!(rows.iter().any(|(identifier, nav_type, icao)| {
            identifier == "NDBZZ" && nav_type == "NDB" && icao == "ZZ"
        }));
    }
}
