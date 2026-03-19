use crate::core::parsers::{for_each_cifp_line, CifpFields};
use crate::core::{
    db::{
        ensure_nav_id_indexes, get_shared_connection, open_sqlite_connection,
        quote_sqlite_identifier, RustSqliteConnection,
    },
    matchers::{
        get_shared_coordinate_cache, get_shared_ref_matcher, CoordinateLookupRequest,
        CoordinateLookupResult, CoordinateSearchType, RefMatchRequest, RefMatchResult,
        RefTableMatcher, SharedCoordinateCache,
    },
};
use anyhow::{anyhow, Result};
use rusqlite::types::Value as SqlValue;
use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::hash::{BuildHasher, Hash};
use std::io::{BufRead, BufReader};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;

static TERMINAL_AIRPORT_FILES_CACHE: OnceLock<Mutex<HashMap<String, Arc<Vec<String>>>>> =
    OnceLock::new();

#[derive(Clone, Debug, PartialEq)]
enum CellValue {
    None,
    Str(Arc<str>),
    Float(f64),
}

const SQLITE_MAX_VARIABLE_NUMBER: usize = 999;
const CIFP_READER_CAPACITY: usize = 256 * 1024;

type MatchCellTuple = (CellValue, CellValue, CellValue);
type MatchCellRow = Arc<MatchCellTuple>;
type RecordRow = Box<[CellValue]>;

#[derive(Default)]
struct MatchCache {
    buckets: HashMap<u64, Vec<(MatchCacheKey, MatchCellRow)>>,
    hash_builder: std::collections::hash_map::RandomState,
}

impl MatchCache {
    fn get(&self, lookup: &MatchCacheLookupKey<'_>) -> Option<&MatchCellRow> {
        let hash = cache_hash(&self.hash_builder, lookup);
        self.buckets.get(&hash).and_then(|entries| {
            entries
                .iter()
                .find(|(key, _)| key.matches_lookup(lookup))
                .map(|(_, row)| row)
        })
    }

    fn insert(&mut self, key: MatchCacheKey, row: MatchCellRow) {
        let hash = cache_hash(&self.hash_builder, &key);
        let entries = self.buckets.entry(hash).or_default();
        if let Some((_, cached_row)) = entries
            .iter_mut()
            .find(|(cached_key, _)| *cached_key == key)
        {
            *cached_row = row;
            return;
        }
        entries.push((key, row));
    }
}

struct ProcedureGroupRows {
    auth_required: bool,
    rows: Vec<RecordRow>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
enum MatchRequestKind {
    Waypoint,
    RecommendedNavaid,
    Center,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct MatchCacheKey {
    kind: MatchRequestKind,
    identifier: Option<Arc<str>>,
    latitude_bits: Option<u64>,
    longitude_bits: Option<u64>,
    is_airport: bool,
    airport_id: Option<Arc<str>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
struct MatchCacheLookupKey<'a> {
    kind: MatchRequestKind,
    identifier: Option<&'a str>,
    latitude_bits: Option<u64>,
    longitude_bits: Option<u64>,
    is_airport: bool,
    airport_id: Option<&'a str>,
}

impl<'a> MatchCacheLookupKey<'a> {
    fn new(
        kind: MatchRequestKind,
        identifier: Option<&'a str>,
        latitude: Option<f64>,
        longitude: Option<f64>,
        is_airport: bool,
        airport_id: Option<&'a str>,
    ) -> Self {
        Self {
            kind,
            identifier,
            latitude_bits: latitude.map(normalized_f64_bits),
            longitude_bits: longitude.map(normalized_f64_bits),
            is_airport,
            airport_id,
        }
    }

    fn to_owned_key(self) -> MatchCacheKey {
        MatchCacheKey {
            kind: self.kind,
            identifier: self.identifier.map(shared_str),
            latitude_bits: self.latitude_bits,
            longitude_bits: self.longitude_bits,
            is_airport: self.is_airport,
            airport_id: self.airport_id.map(shared_str),
        }
    }
}

impl MatchCacheKey {
    fn matches_lookup(&self, lookup: &MatchCacheLookupKey<'_>) -> bool {
        self.kind == lookup.kind
            && self.identifier.as_deref() == lookup.identifier
            && self.latitude_bits == lookup.latitude_bits
            && self.longitude_bits == lookup.longitude_bits
            && self.is_airport == lookup.is_airport
            && self.airport_id.as_deref() == lookup.airport_id
    }
}

#[derive(Clone)]
pub struct TerminalProcedureConfig {
    pub table_name: String,
    pub cifp_prefix: String,
    pub seqno_start: usize,
    pub seqno_end: usize,
    pub airport_prefixes: Vec<String>,
    pub compute_auth: bool,
    pub use_iaps_logic: bool,
    pub batch_size: usize,
    pub min_fields: usize,
}

struct ProcedureBuildContext<'a> {
    airport_identifier: &'a str,
    airport_identifier_cell: CellValue,
    area_code_cell: CellValue,
    procedure_authorization_cell: CellValue,
    iaps_leg_type_cell: CellValue,
    columns: &'a [String],
    authorization_required_index: Option<usize>,
    coord_cache: &'a SharedCoordinateCache,
    matcher: &'a RefTableMatcher,
    match_cache: &'a mut MatchCache,
    batch_records: &'a mut Vec<RecordRow>,
    config: &'a TerminalProcedureConfig,
}

struct RefRequest<'a> {
    lookup_key: MatchCacheLookupKey<'a>,
    request: RefMatchRequest<'a>,
}

impl<'a> RefRequest<'a> {
    fn new(
        kind: MatchRequestKind,
        identifier: Option<&'a str>,
        latitude: Option<f64>,
        longitude: Option<f64>,
        is_airport: bool,
        airport_id: Option<&'a str>,
    ) -> Self {
        Self {
            lookup_key: MatchCacheLookupKey::new(
                kind, identifier, latitude, longitude, is_airport, airport_id,
            ),
            request: RefMatchRequest {
                identifier,
                latitude,
                longitude,
                is_airport,
                airport_id,
            },
        }
    }
}

impl CellValue {
    fn as_str(&self) -> Option<&str> {
        match self {
            Self::Str(value) => Some(value.as_ref()),
            Self::Float(_) | Self::None => None,
        }
    }

    #[cfg(test)]
    fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Float(value) => Some(*value),
            Self::Str(value) => value.parse().ok(),
            Self::None => None,
        }
    }

    #[cfg(test)]
    fn as_upper_str(&self) -> Option<Box<str>> {
        match self {
            Self::Str(value) => Some(value.trim().to_uppercase().into_boxed_str()),
            Self::Float(_) | Self::None => None,
        }
    }
}

fn normalized_f64_bits(number: f64) -> u64 {
    let rounded = ((number * 100_000_000.0).round()) / 100_000_000.0;
    let normalized = if rounded == 0.0 { 0.0 } else { rounded };
    normalized.to_bits()
}

fn extract_opt_field<'a>(parts: &'a CifpFields<'a>, idx: usize) -> Option<&'a str> {
    parts
        .get(idx)
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn extract_opt_field_owned(parts: &CifpFields<'_>, idx: usize) -> Option<String> {
    extract_opt_field(parts, idx).map(str::to_string)
}

fn shared_str(value: impl Into<Arc<str>>) -> Arc<str> {
    value.into()
}

fn airport_file_cache() -> &'static Mutex<HashMap<String, Arc<Vec<String>>>> {
    TERMINAL_AIRPORT_FILES_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn airport_file_cache_key(source_dat_directory: &str, airport_prefixes: &[String]) -> String {
    let mut prefixes = airport_prefixes.to_vec();
    prefixes.sort_unstable();
    format!("{}|{}", source_dat_directory, prefixes.join(","))
}

fn trim_ascii_whitespace_bounds(line: &str, mut start: usize, mut end: usize) -> (usize, usize) {
    let bytes = line.as_bytes();

    while start < end && bytes[start].is_ascii_whitespace() {
        start += 1;
    }
    while start < end && bytes[end - 1].is_ascii_whitespace() {
        end -= 1;
    }

    (start, end)
}

fn collect_required_identifiers_from_line(
    line: &str,
    prefix: &str,
    identifiers: &mut HashSet<Box<str>>,
) {
    if !line.starts_with(prefix) {
        return;
    }

    let line = line.trim_end_matches(['\r', '\n']);
    let target_fields = [4usize, 13usize, 30usize];
    let mut target_index = 0usize;
    let mut field_index = 0usize;
    let mut start = 0usize;

    while target_index < target_fields.len() {
        let end = line[start..]
            .find(',')
            .map_or(line.len(), |offset| start + offset);

        if field_index == target_fields[target_index] {
            let (trimmed_start, trimmed_end) = trim_ascii_whitespace_bounds(line, start, end);
            if trimmed_start < trimmed_end {
                identifiers.insert(line[trimmed_start..trimmed_end].into());
            }
            target_index += 1;
        }

        if end == line.len() {
            break;
        }

        field_index += 1;
        start = end + 1;
    }
}

fn collect_required_identifiers_from_reader<R: BufRead>(
    mut reader: R,
    prefix: &str,
    identifiers: &mut HashSet<Box<str>>,
) -> Result<()> {
    let mut line = String::new();

    loop {
        line.clear();
        if reader.read_line(&mut line)? == 0 {
            break;
        }
        collect_required_identifiers_from_line(&line, prefix, identifiers);
    }

    Ok(())
}

fn string_cell<T>(value: Option<T>) -> CellValue
where
    T: Into<Arc<str>>,
{
    value
        .map(Into::into)
        .map_or(CellValue::None, CellValue::Str)
}

fn parse_altitude(alt_str: &str) -> Option<Arc<str>> {
    if alt_str.trim().is_empty() {
        return None;
    }
    let alt = alt_str.trim();
    if let Some(fl_value) = alt.strip_prefix("FL") {
        return fl_value
            .parse::<i64>()
            .ok()
            .map(|value| shared_str((value * 100).to_string()));
    }
    Some(shared_str(alt))
}

fn convert_rnp(rnp: &str) -> Option<f64> {
    let trimmed = rnp.trim();
    if trimmed.len() != 3 || !trimmed.chars().all(|ch| ch.is_ascii_digit()) {
        return None;
    }
    let numerator = trimmed[..2].parse::<f64>().ok()?;
    let exponent = trimmed[2..].parse::<i32>().ok()?;
    Some(numerator / 10f64.powi(exponent))
}

fn convert_divided_by(value: &str, divisor: f64) -> Option<f64> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }
    trimmed.parse::<f64>().ok().map(|number| number / divisor)
}

fn convert_vertical_angle(value: &str) -> Option<f64> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }
    let parsed = trimmed.parse::<f64>().ok()? / 100.0;
    Some((parsed * 10.0).round() / 10.0)
}

fn type_check(waypoint: Option<&str>, icao_code: Option<&str>) -> bool {
    waypoint.is_some_and(|value| {
        let trimmed = value.trim();
        trimmed.len() == 4
            && trimmed.starts_with('Z')
            && !matches!(icao_code.map(str::trim), Some("ZZ"))
    })
}

fn get_area_code(airport_identifier: &str) -> &'static str {
    if airport_identifier.starts_with("OP") {
        return "MES";
    }
    if airport_identifier.starts_with("VH") {
        return "PAC";
    }
    "EEU"
}

fn scan_airport_files(
    source_dat_directory: &str,
    airport_prefixes: &[String],
) -> Result<Vec<String>> {
    let cache_key = airport_file_cache_key(source_dat_directory, airport_prefixes);
    let cached_files = airport_file_cache()
        .lock()
        .unwrap()
        .get(&cache_key)
        .cloned();
    if let Some(cached) = cached_files {
        return Ok((*cached).clone());
    }

    let mut out = Vec::new();
    let entries = fs::read_dir(source_dat_directory).map_err(|err| {
        anyhow!(
            "failed to read source_dat_directory {source_dat_directory}: {err}"
        )
    })?;

    for entry in entries {
        let entry =
            entry.map_err(|err| anyhow!("failed to iterate source_dat_directory: {err}"))?;
        let file_name = entry.file_name();
        let file_name = file_name.to_string_lossy();
        if !file_name.ends_with(".dat") {
            continue;
        }
        if airport_prefixes
            .iter()
            .any(|prefix| file_name.starts_with(prefix))
        {
            out.push(file_name.to_string());
        }
    }

    out.sort_unstable();
    airport_file_cache()
        .lock()
        .unwrap()
        .insert(cache_key, Arc::new(out.clone()));

    Ok(out)
}

fn collect_terminal_required_identifiers(
    source_dat_directory: &str,
    airport_files: &[String],
    config: &TerminalProcedureConfig,
) -> Result<Arc<HashSet<Box<str>>>> {
    let mut identifiers = HashSet::new();

    for filename in airport_files {
        let full_path = std::path::Path::new(source_dat_directory).join(filename);
        let file = File::open(&full_path)
            .map_err(|err| anyhow!("failed to open {}: {}", full_path.display(), err))?;
        let reader = BufReader::with_capacity(CIFP_READER_CAPACITY, file);
        collect_required_identifiers_from_reader(reader, &config.cifp_prefix, &mut identifiers)?;
    }

    Ok(Arc::new(identifiers))
}

fn build_insert_sql(table_name: &str) -> Result<String> {
    let sql = match table_name {
        "tbl_sids" => "INSERT OR IGNORE INTO tbl_sids (area_code, airport_identifier, procedure_identifier, route_type, transition_identifier, seqno, waypoint_icao_code, waypoint_identifier, waypoint_latitude, waypoint_longitude, waypoint_description_code, turn_direction, rnp, path_termination, recommanded_navaid, recommanded_navaid_latitude, recommanded_navaid_longitude, arc_radius, theta, rho, magnetic_course, route_distance_holding_distance_time, distance_time, altitude_description, altitude1, altitude2, transition_altitude, speed_limit_description, speed_limit, vertical_angle, center_waypoint, center_waypoint_latitude, center_waypoint_longitude, aircraft_category, id, recommanded_id, center_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        "tbl_stars" => "INSERT OR IGNORE INTO tbl_stars (area_code, airport_identifier, procedure_identifier, route_type, transition_identifier, seqno, waypoint_icao_code, waypoint_identifier, waypoint_latitude, waypoint_longitude, waypoint_description_code, turn_direction, rnp, path_termination, recommanded_navaid, recommanded_navaid_latitude, recommanded_navaid_longitude, arc_radius, theta, rho, magnetic_course, route_distance_holding_distance_time, distance_time, altitude_description, altitude1, altitude2, transition_altitude, speed_limit_description, speed_limit, vertical_angle, center_waypoint, center_waypoint_latitude, center_waypoint_longitude, aircraft_category, id, recommanded_id, center_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        "tbl_iaps" => "INSERT OR IGNORE INTO tbl_iaps (area_code, airport_identifier, procedure_identifier, route_type, transition_identifier, seqno, waypoint_icao_code, waypoint_identifier, waypoint_latitude, waypoint_longitude, waypoint_description_code, turn_direction, rnp, path_termination, recommanded_navaid, recommanded_navaid_latitude, recommanded_navaid_longitude, arc_radius, theta, rho, magnetic_course, route_distance_holding_distance_time, distance_time, altitude_description, altitude1, altitude2, transition_altitude, speed_limit_description, speed_limit, vertical_angle, center_waypoint, center_waypoint_latitude, center_waypoint_longitude, aircraft_category, id, recommanded_id, center_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        _ => return Err(anyhow!("unsupported procedure table: {table_name}")),
    };
    Ok(sql.to_string())
}

fn procedure_columns(table_name: &str) -> Result<Vec<String>> {
    let columns = match table_name {
        "tbl_sids" | "tbl_stars" | "tbl_iaps" => vec![
            "area_code",
            "airport_identifier",
            "procedure_identifier",
            "route_type",
            "transition_identifier",
            "seqno",
            "waypoint_icao_code",
            "waypoint_identifier",
            "waypoint_latitude",
            "waypoint_longitude",
            "waypoint_description_code",
            "turn_direction",
            "rnp",
            "path_termination",
            "recommanded_navaid",
            "recommanded_navaid_latitude",
            "recommanded_navaid_longitude",
            "arc_radius",
            "theta",
            "rho",
            "magnetic_course",
            "route_distance_holding_distance_time",
            "distance_time",
            "altitude_description",
            "altitude1",
            "altitude2",
            "transition_altitude",
            "speed_limit_description",
            "speed_limit",
            "vertical_angle",
            "center_waypoint",
            "center_waypoint_latitude",
            "center_waypoint_longitude",
            "aircraft_category",
            "id",
            "recommanded_id",
            "center_id",
        ],
        _ => return Err(anyhow!("unsupported procedure table: {table_name}")),
    };
    Ok(columns.into_iter().map(str::to_string).collect())
}

fn load_existing_proc_map_from_conn(
    conn: &RustSqliteConnection,
    table_name: &str,
    airport_identifiers: &[String],
) -> Result<HashMap<String, HashSet<Option<String>>>> {
    let mut out: HashMap<String, HashSet<Option<String>>> = HashMap::new();
    if airport_identifiers.is_empty() {
        return Ok(out);
    }

    for chunk in airport_identifiers.chunks(SQLITE_MAX_VARIABLE_NUMBER) {
        let placeholders = vec!["?"; chunk.len()].join(", ");
        let query = format!(
            "SELECT airport_identifier, procedure_identifier FROM {} WHERE airport_identifier IN ({})",
            quote_sqlite_identifier(table_name),
            placeholders,
        );
        let params = chunk
            .iter()
            .map(|airport_identifier| SqlValue::Text(airport_identifier.clone()))
            .collect::<Vec<_>>();
        conn.query_each_native(&query, &params, |row| {
            let airport_id: String = row.get(0)?;
            let procedure_id: Option<String> = row.get(1)?;
            out.entry(airport_id).or_default().insert(procedure_id);
            Ok(())
        })?;
    }
    Ok(out)
}

fn delete_zuls_special_procedures(conn: &RustSqliteConnection, table_name: &str) -> Result<()> {
    const ZULS: &str = "ZULS";

    match table_name {
        "tbl_sids" => {
            let query = format!(
                "DELETE FROM {} WHERE airport_identifier = ?1 AND (procedure_identifier LIKE ?2 OR procedure_identifier LIKE ?3)",
                quote_sqlite_identifier(table_name),
            );
            conn.execute_statement_native(
                &query,
                &[
                    SqlValue::Text(ZULS.to_string()),
                    SqlValue::Text("DEP%".to_string()),
                    SqlValue::Text("EO%".to_string()),
                ],
            )?;
        }
        "tbl_stars" => {
            const TO_REMOVE: &[&str] = &[
                "DUM08A",
                "DUM09A",
                "DUM10A",
                "DUM28A",
                "IGD10A",
                "IGD28A",
                "IKU10A",
                "IKU28A",
            ];
            let placeholders = vec!["?"; TO_REMOVE.len()].join(", ");
            let query = format!(
                "DELETE FROM {} WHERE airport_identifier = ?1 AND procedure_identifier IN ({})",
                quote_sqlite_identifier(table_name),
                placeholders,
            );
            let mut params: Vec<SqlValue> = Vec::with_capacity(1 + TO_REMOVE.len());
            params.push(SqlValue::Text(ZULS.to_string()));
            for id in TO_REMOVE {
                params.push(SqlValue::Text(id.to_string()));
            }
            conn.execute_statement_native(&query, &params)?;
        }
        "tbl_iaps" => {
            let query = format!(
                "DELETE FROM {} WHERE airport_identifier = ?1 AND procedure_identifier LIKE ?2",
                quote_sqlite_identifier(table_name),
            );
            conn.execute_statement_native(
                &query,
                &[
                    SqlValue::Text(ZULS.to_string()),
                    SqlValue::Text("R%".to_string()),
                ],
            )?;
        }
        _ => {
            // not applicable
        }
    }

    Ok(())
}

fn prepare_existing_proc_map(
    conn: &RustSqliteConnection,
    table_name: &str,
    airport_identifiers: &[String],
) -> Result<HashMap<String, HashSet<Option<String>>>> {
    if airport_identifiers.iter().any(|airport_identifier| airport_identifier == "ZULS") {
        delete_zuls_special_procedures(conn, table_name)?;
    }

    load_existing_proc_map_from_conn(conn, table_name, airport_identifiers)
}

fn build_reference_id_cell(
    ref_table: &CellValue,
    identifier: Option<&str>,
    icao_code: Option<&str>,
    airport_identifier: &str,
) -> CellValue {
    let Some(ref_table) = ref_table.as_str() else {
        return CellValue::None;
    };
    let Some(identifier) = identifier.map(str::trim).filter(|value| !value.is_empty()) else {
        return CellValue::None;
    };
    let Some(icao_code) = icao_code.map(str::trim).filter(|value| !value.is_empty()) else {
        return CellValue::None;
    };

    let raw_id = match ref_table {
        "tbl_airports" | "tbl_enroute_ndbnavaids" | "tbl_enroute_waypoints" | "tbl_vhfnavaids" => {
            format!("{icao_code}{identifier}")
        }
        "tbl_terminal_ndbnavaids"
        | "tbl_terminal_waypoints"
        | "tbl_runways"
        | "tbl_localizers_glideslopes"
        | "tbl_gls" => format!("{airport_identifier}{icao_code}{identifier}"),
        _ => return CellValue::None,
    };

    CellValue::Str(shared_str(format!("{ref_table}|{raw_id}")))
}

fn find_coordinates_with_cache(
    coord_cache: &SharedCoordinateCache,
    search_type: CoordinateSearchType,
    identifier: Option<&str>,
    icao_code: Option<&str>,
    region_code: Option<&str>,
) -> CoordinateLookupResult {
    coord_cache.find_coordinates_native(CoordinateLookupRequest {
        search_type,
        identifier,
        icao_code,
        region_code,
    })
}

fn matched_row_to_cells(row: RefMatchResult) -> MatchCellTuple {
    (
        row.ref_table
            .map(shared_str)
            .map_or(CellValue::None, CellValue::Str),
        row.latitude
            .map_or(CellValue::None, CellValue::Float),
        row.longitude
            .map_or(CellValue::None, CellValue::Float),
    )
}

const fn resolve_match_row(matched: MatchCellTuple) -> MatchCellTuple {
    matched
}

fn clone_match_cells(row: &MatchCellRow) -> MatchCellTuple {
    let (ref_table, latitude, longitude) = row.as_ref();
    (ref_table.clone(), latitude.clone(), longitude.clone())
}

fn cache_hash<S, T>(hash_builder: &S, value: &T) -> u64
where
    S: BuildHasher,
    T: Hash,
{
    hash_builder.hash_one(value)
}

fn match_ref_requests<'a, const N: usize>(
    matcher: &RefTableMatcher,
    match_cache: &mut MatchCache,
    requests: [RefRequest<'a>; N],
) -> [MatchCellRow; N] {
    let mut results: [Option<MatchCellRow>; N] = std::array::from_fn(|_| None);
    let mut misses: [Option<RefMatchRequest<'a>>; N] = std::array::from_fn(|_| None);
    let mut miss_keys: [Option<MatchCacheKey>; N] = std::array::from_fn(|_| None);
    let mut miss_indices = [0usize; N];
    let mut miss_count = 0usize;

    for (idx, request) in requests.into_iter().enumerate() {
        if let Some(cached) = match_cache.get(&request.lookup_key) {
            results[idx] = Some(Arc::clone(cached));
            continue;
        }

        let RefRequest {
            lookup_key,
            request,
        } = request;

        miss_indices[miss_count] = idx;
        miss_keys[miss_count] = Some(lookup_key.to_owned_key());
        misses[miss_count] = Some(request);
        miss_count += 1;
    }

    if miss_count != 0 {
        let matched_rows =
            matcher.match_batch_native(misses.into_iter().take(miss_count).flatten());
        for output_index in 0..miss_count {
            let matched_index = miss_indices[output_index];
            let row = Arc::new(resolve_match_row(
                matched_rows
                    .get(output_index)
                    .copied()
                    .map_or((CellValue::None, CellValue::None, CellValue::None), matched_row_to_cells),
            ));
            match_cache.insert(miss_keys[output_index].take().unwrap(), Arc::clone(&row));
            results[matched_index] = Some(row);
        }
    }

    std::array::from_fn(|idx| {
        results[idx]
            .take()
            .unwrap_or_else(|| Arc::new((CellValue::None, CellValue::None, CellValue::None)))
    })
}

fn row_requires_authorization(rnp: Option<f64>, path_termination: Option<&str>) -> bool {
    match rnp {
        Some(value) if value < 0.3 => true,
        Some(value) if (value - 0.3).abs() < f64::EPSILON => {
            path_termination.is_some_and(|path| path.trim().eq_ignore_ascii_case("RF"))
        }
        _ => false,
    }
}

struct MatchedProcedureCells<'a> {
    waypoint_identifier: Option<&'a str>,
    waypoint_icao_code: Option<&'a str>,
    center_waypoint: Option<&'a str>,
    center_waypoint_icao_code: Option<&'a str>,
    recommended_navaid: Option<&'a str>,
    recommended_navaid_icao_code: Option<&'a str>,
    waypoint_ref_table: CellValue,
    waypoint_latitude: CellValue,
    waypoint_longitude: CellValue,
    recommended_navaid_ref_table: CellValue,
    recommended_navaid_latitude: CellValue,
    recommended_navaid_longitude: CellValue,
    center_waypoint_ref_table: CellValue,
    center_waypoint_latitude: CellValue,
    center_waypoint_longitude: CellValue,
    waypoint_id: CellValue,
    recommended_id: CellValue,
    center_id: CellValue,
}

struct TerminalRowFields<'a> {
    procedure_identifier: Option<String>,
    group_key: Option<String>,
    row_auth_required: bool,
    route_type: Option<&'a str>,
    transition_identifier: Option<&'a str>,
    seqno: Option<&'a str>,
    waypoint_description_code: Option<&'a str>,
    turn_direction: Option<&'a str>,
    path_termination: Option<&'a str>,
    altitude_description: Option<Arc<str>>,
    altitude1: Option<Arc<str>>,
    altitude2: Option<Arc<str>>,
    transition_altitude: Option<&'a str>,
    arc_radius: Option<f64>,
    course: Option<f64>,
    rho: Option<f64>,
    theta: Option<f64>,
    rnp: Option<f64>,
    route_distance: Option<f64>,
    speed_limit: Option<&'a str>,
    speed_limit_description: Option<&'a str>,
    vertical_angle: Option<f64>,
    course_flag: Option<&'static str>,
    distance_time: Option<&'static str>,
}

fn resolve_matched_procedure_cells<'a>(
    parts: &'a CifpFields<'a>,
    context: &mut ProcedureBuildContext<'_>,
) -> MatchedProcedureCells<'a> {
    let waypoint_identifier = extract_opt_field(parts, 4);
    let waypoint_icao_code = extract_opt_field(parts, 5);
    let waypoint_coordinates = find_coordinates_with_cache(
        context.coord_cache,
        CoordinateSearchType::Waypoint,
        waypoint_identifier,
        waypoint_icao_code,
        Some(context.airport_identifier),
    );

    let recommended_navaid = extract_opt_field(parts, 13);
    let recommended_navaid_icao_code = recommended_navaid.and(waypoint_icao_code);
    let recommended_navaid_coordinates = find_coordinates_with_cache(
        context.coord_cache,
        CoordinateSearchType::RecommendedNavaid,
        recommended_navaid,
        recommended_navaid_icao_code,
        None,
    );

    let center_waypoint = extract_opt_field(parts, 30);
    let center_waypoint_icao_code = center_waypoint.and(waypoint_icao_code);
    let center_waypoint_coordinates = find_coordinates_with_cache(
        context.coord_cache,
        CoordinateSearchType::Center,
        center_waypoint,
        center_waypoint_icao_code,
        Some(context.airport_identifier),
    );

    let match_airport_id = Some(context.airport_identifier);
    let requests = [
        RefRequest::new(
            MatchRequestKind::Waypoint,
            waypoint_identifier,
            waypoint_coordinates.latitude,
            waypoint_coordinates.longitude,
            type_check(waypoint_identifier, waypoint_icao_code),
            match_airport_id,
        ),
        RefRequest::new(
            MatchRequestKind::RecommendedNavaid,
            recommended_navaid,
            recommended_navaid_coordinates.latitude,
            recommended_navaid_coordinates.longitude,
            false,
            match_airport_id,
        ),
        RefRequest::new(
            MatchRequestKind::Center,
            center_waypoint,
            center_waypoint_coordinates.latitude,
            center_waypoint_coordinates.longitude,
            type_check(center_waypoint, center_waypoint_icao_code),
            match_airport_id,
        ),
    ];

    let matched_rows = match_ref_requests(context.matcher, context.match_cache, requests);
    let (waypoint_ref_table, waypoint_latitude, waypoint_longitude) = clone_match_cells(&matched_rows[0]);
    let (recommended_navaid_ref_table, recommended_navaid_latitude, recommended_navaid_longitude) =
        clone_match_cells(&matched_rows[1]);
    let (center_waypoint_ref_table, center_waypoint_latitude, center_waypoint_longitude) =
        clone_match_cells(&matched_rows[2]);

    MatchedProcedureCells {
        waypoint_identifier,
        waypoint_icao_code,
        center_waypoint,
        center_waypoint_icao_code,
        recommended_navaid,
        recommended_navaid_icao_code,
        waypoint_id: build_reference_id_cell(
            &waypoint_ref_table,
            waypoint_identifier,
            waypoint_icao_code,
            context.airport_identifier,
        ),
        recommended_id: build_reference_id_cell(
            &recommended_navaid_ref_table,
            recommended_navaid,
            recommended_navaid_icao_code,
            context.airport_identifier,
        ),
        center_id: build_reference_id_cell(
            &center_waypoint_ref_table,
            center_waypoint,
            center_waypoint_icao_code,
            context.airport_identifier,
        ),
        waypoint_ref_table,
        waypoint_latitude,
        waypoint_longitude,
        recommended_navaid_ref_table,
        recommended_navaid_latitude,
        recommended_navaid_longitude,
        center_waypoint_ref_table,
        center_waypoint_latitude,
        center_waypoint_longitude,
    }
}

fn collect_terminal_row_fields<'a>(
    parts: &'a CifpFields<'a>,
    procedure_identifier: Option<String>,
    needs_grouping: bool,
    seqno_start: usize,
    seqno_end: usize,
) -> TerminalRowFields<'a> {
    let route_type = extract_opt_field(parts, 1);
    let transition_identifier = extract_opt_field(parts, 3);
    let seq_source = parts.first().unwrap_or_default();
    let seqno = seq_source
        .get(seqno_start..seqno_end)
        .map(str::trim)
        .filter(|value| !value.is_empty());

    let waypoint_description_code = extract_opt_field(parts, 8);
    let turn_direction = extract_opt_field(parts, 9);
    let path_termination = extract_opt_field(parts, 11);

    let mut altitude_description = extract_opt_field(parts, 22).map(shared_str);
    let altitude1 = parts.get(23).and_then(parse_altitude);
    let altitude2 = parts.get(24).and_then(parse_altitude);
    if altitude1.is_some() && altitude2.is_none() && altitude_description.is_none() {
        altitude_description = Some(shared_str("@"));
    }

    let transition_altitude = extract_opt_field(parts, 25);
    let arc_radius = parts.get(17).and_then(|value| convert_divided_by(value, 1000.0));
    let course = parts.get(20).and_then(|value| convert_divided_by(value, 10.0));
    let rho = parts.get(19).and_then(|value| convert_divided_by(value, 10.0));
    let theta = parts.get(18).and_then(|value| convert_divided_by(value, 10.0));
    let rnp = parts.get(10).and_then(convert_rnp);
    let route_distance = parts.get(21).and_then(|value| convert_divided_by(value, 10.0));
    let speed_limit = extract_opt_field(parts, 27);
    let speed_limit_description = extract_opt_field(parts, 26);
    let vertical_angle = parts.get(28).and_then(convert_vertical_angle);

    TerminalRowFields {
        row_auth_required: row_requires_authorization(rnp, path_termination),
        group_key: needs_grouping.then_some(procedure_identifier.clone()).flatten(),
        course_flag: course.is_some().then_some("M"),
        distance_time: route_distance.is_some().then_some("D"),
        procedure_identifier,
        route_type,
        transition_identifier,
        seqno,
        waypoint_description_code,
        turn_direction,
        path_termination,
        altitude_description,
        altitude1,
        altitude2,
        transition_altitude,
        arc_radius,
        course,
        rho,
        theta,
        rnp,
        route_distance,
        speed_limit,
        speed_limit_description,
        vertical_angle,
    }
}

fn build_terminal_record_row(
    context: &ProcedureBuildContext<'_>,
    fields: &TerminalRowFields<'_>,
    cells: &MatchedProcedureCells<'_>,
) -> RecordRow {
    context
        .columns
        .iter()
        .map(|column| match column.as_str() {
            "airport_identifier" => context.airport_identifier_cell.clone(),
            "altitude_description" => string_cell(fields.altitude_description.clone()),
            "altitude1" => string_cell(fields.altitude1.clone()),
            "altitude2" => string_cell(fields.altitude2.clone()),
            "arc_radius" => fields.arc_radius.map_or(CellValue::None, CellValue::Float),
            "area_code" => context.area_code_cell.clone(),
            "center_waypoint_icao_code" => string_cell(cells.center_waypoint_icao_code),
            "center_waypoint_latitude" => cells.center_waypoint_latitude.clone(),
            "center_waypoint_longitude" => cells.center_waypoint_longitude.clone(),
            "center_waypoint_ref_table" => cells.center_waypoint_ref_table.clone(),
            "center_waypoint" => string_cell(cells.center_waypoint),
            "course_flag" => string_cell(fields.course_flag),
            "course" | "magnetic_course" => fields.course.map_or(CellValue::None, CellValue::Float),
            "ctl" => {
                if context.config.use_iaps_logic {
                    context.iaps_leg_type_cell.clone()
                } else {
                    CellValue::None
                }
            }
            "distance_time" => string_cell(fields.distance_time),
            "path_termination" => string_cell(fields.path_termination),
            "procedure_identifier" => string_cell(fields.procedure_identifier.as_deref()),
            "recommended_navaid_icao_code" => string_cell(cells.recommended_navaid_icao_code),
            "recommended_navaid_latitude" | "recommanded_navaid_latitude" => {
                cells.recommended_navaid_latitude.clone()
            }
            "recommended_navaid_longitude" | "recommanded_navaid_longitude" => {
                cells.recommended_navaid_longitude.clone()
            }
            "recommended_navaid_ref_table" => cells.recommended_navaid_ref_table.clone(),
            "recommended_navaid" | "recommanded_navaid" => string_cell(cells.recommended_navaid),
            "recommended_navaid_id" | "recommanded_id" => cells.recommended_id.clone(),
            "rho" => fields.rho.map_or(CellValue::None, CellValue::Float),
            "rnp" => fields.rnp.map_or(CellValue::None, CellValue::Float),
            "route_distance_holding_distance_time" => {
                fields.route_distance.map_or(CellValue::None, CellValue::Float)
            }
            "route_type" => string_cell(fields.route_type),
            "seqno" => string_cell(fields.seqno),
            "speed_limit_description" => string_cell(fields.speed_limit_description),
            "speed_limit" => string_cell(fields.speed_limit),
            "theta" => fields.theta.map_or(CellValue::None, CellValue::Float),
            "transition_altitude" => string_cell(fields.transition_altitude),
            "transition_identifier" => string_cell(fields.transition_identifier),
            "turn_direction" => string_cell(fields.turn_direction),
            "vertical_angle" => fields.vertical_angle.map_or(CellValue::None, CellValue::Float),
            "waypoint_description_code" => string_cell(fields.waypoint_description_code),
            "waypoint_icao_code" => string_cell(cells.waypoint_icao_code),
            "waypoint_identifier" => string_cell(cells.waypoint_identifier),
            "waypoint_latitude" => cells.waypoint_latitude.clone(),
            "waypoint_longitude" => cells.waypoint_longitude.clone(),
            "waypoint_ref_table" => cells.waypoint_ref_table.clone(),
            "center_id" => cells.center_id.clone(),
            "id" => cells.waypoint_id.clone(),
            "aircraft_category" => string_cell(Some("J")),
            _ => CellValue::None,
        })
        .collect::<Vec<_>>()
        .into_boxed_slice()
}

fn build_terminal_cifp_records_with_matcher<R: BufRead>(
    reader: R,
    context: &mut ProcedureBuildContext<'_>,
    existing_procedures: Option<&HashSet<Option<String>>>,
) -> Result<usize> {
    let mut total_processed = 0usize;
    let mut grouped_records: HashMap<Option<String>, ProcedureGroupRows> = HashMap::new();
    let needs_grouping = context.config.use_iaps_logic || context.config.compute_auth;

    for_each_cifp_line(
        reader,
        &context.config.cifp_prefix,
        context.config.min_fields,
        |parts| {
            let procedure_identifier = extract_opt_field_owned(&parts, 2);
            if existing_procedures.is_some_and(|existing| existing.contains(&procedure_identifier))
            {
                return Ok(());
            }
            let matched_cells = resolve_matched_procedure_cells(&parts, context);
            let row_fields = collect_terminal_row_fields(
                &parts,
                procedure_identifier,
                needs_grouping,
                context.config.seqno_start,
                context.config.seqno_end,
            );
            let row = build_terminal_record_row(context, &row_fields, &matched_cells);

            if needs_grouping {
                let group_key = row_fields.group_key.clone();
                let group =
                    grouped_records
                        .entry(group_key)
                        .or_insert_with(|| ProcedureGroupRows {
                            auth_required: false,
                            rows: Vec::new(),
                        });
                group.auth_required |= row_fields.row_auth_required;
                group.rows.push(row);
            } else {
                context.batch_records.push(row);
            }
            total_processed += 1;
            Ok(())
        },
    )?;

    if needs_grouping {
        for (_, mut group) in grouped_records {
            if group.auth_required {
                if let Some(index) = context.authorization_required_index {
                    for row in &mut group.rows {
                        row[index] = context.procedure_authorization_cell.clone();
                    }
                }
            }
            context.batch_records.extend(group.rows);
        }
    }

    Ok(total_processed)
}

fn bind_cell_row(stmt: &mut rusqlite::Statement<'_>, row: &[CellValue]) -> rusqlite::Result<()> {
    for (idx, cell) in row.iter().enumerate() {
        let param: usize = idx + 1;
        match cell {
            CellValue::None => stmt.raw_bind_parameter(param, rusqlite::types::Null)?,
            CellValue::Str(s) => stmt.raw_bind_parameter(param, s.as_ref())?,
            CellValue::Float(f) => stmt.raw_bind_parameter(param, *f)?,
        }
    }
    stmt.raw_execute()?;
    Ok(())
}

fn flush_batch_records_binding(
    conn: &RustSqliteConnection,
    query: &str,
    batch_records: &mut Vec<RecordRow>,
    batch_limit: usize,
) -> Result<()> {
    if batch_records.is_empty() {
        return Ok(());
    }

    let batch_limit = batch_limit.max(1);
    conn.with_connection_native(|raw_conn| {
        for chunk in batch_records.chunks(batch_limit) {
            let tx = raw_conn.unchecked_transaction()?;
            {
                let mut stmt = tx.prepare(query)?;
                for row in chunk {
                    bind_cell_row(&mut stmt, row)?;
                }
            }
            tx.commit()?;
        }
        Ok(())
    })?;
    batch_records.clear();
    Ok(())
}

fn load_terminal_matchers(
    earth_fix_path: Option<String>,
    earth_nav_path: Option<String>,
    db_path: &str,
    timeout: u32,
    required_identifiers: Arc<HashSet<Box<str>>>,
) -> Result<(Arc<SharedCoordinateCache>, Arc<RefTableMatcher>)> {
    let db_path = db_path.to_owned();
    let matcher_required_identifiers = Arc::clone(&required_identifiers);
    let coord_handle = thread::spawn(move || {
        get_shared_coordinate_cache(
            earth_fix_path.as_deref(),
            earth_nav_path.as_deref(),
            Some(&required_identifiers),
        )
    });
    let matcher_handle = thread::spawn(move || {
        get_shared_ref_matcher(&db_path, timeout, Some(&matcher_required_identifiers))
    });

    let coord_cache = coord_handle
        .join()
        .map_err(|_| anyhow!("coordinate cache worker panicked"))??;
    let matcher = matcher_handle
        .join()
        .map_err(|_| anyhow!("ref matcher worker panicked"))??;
    Ok((coord_cache, matcher))
}

#[cfg(test)]
fn compute_authorization_required(
    group_rows: &[RecordRow],
    rnp_index: usize,
    path_index: Option<usize>,
) -> Option<String> {
    for row in group_rows {
        let Some(rnp_value) = row.get(rnp_index).and_then(CellValue::as_f64) else {
            continue;
        };

        if rnp_value < 0.3 {
            return Some("Y".to_string());
        }

        if (rnp_value - 0.3).abs() < f64::EPSILON {
            let Some(path_idx) = path_index else {
                continue;
            };
            let Some(path) = row.get(path_idx).and_then(CellValue::as_upper_str) else {
                continue;
            };
            if path.as_ref() == "RF" {
                return Some("Y".to_string());
            }
        }
    }

    None
}

fn convert_terminal_cifp_to_db(
    source_dat_directory: &str,
    airport_files: &[String],
    coord_cache: &SharedCoordinateCache,
    matcher: &RefTableMatcher,
    conn: &RustSqliteConnection,
    config: &TerminalProcedureConfig,
) -> Result<(usize, usize)> {
    let mut airport_identifiers = Vec::with_capacity(airport_files.len());
    let mut seen_airports = HashSet::with_capacity(airport_files.len());
    for filename in airport_files {
        let airport_identifier = filename.split('.').next().unwrap_or_default();
        if !airport_identifier.is_empty() && seen_airports.insert(airport_identifier.to_owned()) {
            airport_identifiers.push(airport_identifier.to_owned());
        }
    }
    let existing_proc_map =
        prepare_existing_proc_map(conn, &config.table_name, &airport_identifiers)?;

    let columns = procedure_columns(&config.table_name)?;
    let authorization_required_index = columns
        .iter()
        .position(|column| column == "authorization_required");
    let query = build_insert_sql(&config.table_name)?;

    let mut total_processed = 0usize;
    let batch_limit = config.batch_size.max(1);
    let mut batch_records: Vec<RecordRow> = Vec::with_capacity(batch_limit);
    let mut match_cache = MatchCache::default();

    for filename in airport_files {
        let airport_identifier = filename.split('.').next().unwrap_or_default();
        let area_code = get_area_code(airport_identifier);
        let full_path = std::path::Path::new(source_dat_directory).join(filename);
        let file = File::open(&full_path)
            .map_err(|err| anyhow!("failed to open {}: {}", full_path.display(), err))?;
        let reader = BufReader::with_capacity(CIFP_READER_CAPACITY, file);
        let mut build_context = ProcedureBuildContext {
            airport_identifier,
            airport_identifier_cell: CellValue::Str(shared_str(airport_identifier)),
            area_code_cell: CellValue::Str(shared_str(area_code)),
            procedure_authorization_cell: CellValue::Str(shared_str("Y")),
            iaps_leg_type_cell: CellValue::Str(shared_str("N")),
            columns: &columns,
            authorization_required_index,
            coord_cache,
            matcher,
            match_cache: &mut match_cache,
            batch_records: &mut batch_records,
            config,
        };

        let processed_count = build_terminal_cifp_records_with_matcher(
            reader,
            &mut build_context,
            existing_proc_map.get(airport_identifier),
        )?;

        total_processed += processed_count;

        if batch_records.len() >= batch_limit {
            flush_batch_records_binding(conn, &query, &mut batch_records, batch_limit)?;
        }
    }

    flush_batch_records_binding(conn, &query, &mut batch_records, batch_limit)?;
    Ok((airport_files.len(), total_processed))
}

pub fn process_terminal_cifp_to_db(
    source_dat_directory: &str,
    earth_fix_path: Option<String>,
    earth_nav_path: Option<String>,
    db_path: &str,
    config: &TerminalProcedureConfig,
    timeout: u32,
) -> Result<(usize, usize)> {
    ensure_nav_id_indexes(db_path, timeout)?;

    let airport_files = scan_airport_files(source_dat_directory, &config.airport_prefixes)?;
    let required_identifiers =
        collect_terminal_required_identifiers(source_dat_directory, &airport_files, config)?;

    let (coord_cache, matcher) = load_terminal_matchers(
        earth_fix_path,
        earth_nav_path,
        db_path,
        timeout,
        required_identifiers,
    )?;
    let shared_conn = get_shared_connection(db_path);
    let owns_connection = shared_conn.is_none();
    let conn = match shared_conn {
        Some(conn) => conn,
        None => open_sqlite_connection(db_path, timeout)?,
    };

    let result = convert_terminal_cifp_to_db(
        source_dat_directory,
        &airport_files,
        &coord_cache,
        &matcher,
        &conn,
        config,
    );

    if owns_connection {
        conn.close_native();
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn computes_authorization_required_like_python() {
        let rows = vec![
            vec![
                CellValue::None,
                CellValue::None,
                CellValue::Float(0.3),
                CellValue::Str(shared_str("RF")),
            ]
            .into_boxed_slice(),
            vec![
                CellValue::None,
                CellValue::None,
                CellValue::Float(1.0),
                CellValue::Str(shared_str("TF")),
            ]
            .into_boxed_slice(),
        ];
        assert_eq!(
            compute_authorization_required(&rows, 2, Some(3)).as_deref(),
            Some("Y")
        );
    }

    #[test]
    fn parses_altitude_and_numeric_converters() {
        assert_eq!(parse_altitude("FL300").as_deref(), Some("30000"));
        assert_eq!(parse_altitude("12345").as_deref(), Some("12345"));
        assert_eq!(convert_rnp("302"), Some(0.3));
        assert_eq!(convert_divided_by("123", 10.0), Some(12.3));
        assert_eq!(convert_vertical_angle("315"), Some(3.2));
    }

    #[test]
    fn scans_airport_files_with_prefix_filter() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("pmdg_navdata_cli_terminal_test_{}", unique));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("ZBAA.dat"), "x").unwrap();
        std::fs::write(dir.join("KJFK.dat"), "x").unwrap();
        std::fs::write(dir.join("ZSPD.txt"), "x").unwrap();

        let mut files =
            scan_airport_files(dir.to_str().unwrap(), &["ZB".to_string(), "ZS".to_string()])
                .unwrap();
        files.sort();

        std::fs::remove_dir_all(&dir).unwrap();
        assert_eq!(files, vec!["ZBAA.dat".to_string()]);
    }

    #[test]
    fn keeps_missing_coordinates_empty_when_ref_match_fails() {
        let matched = (CellValue::None, CellValue::None, CellValue::None);
        let resolved = resolve_match_row(matched);

        assert_eq!(
            resolved,
            (CellValue::None, CellValue::None, CellValue::None)
        );
    }

    #[test]
    fn match_cache_key_normalizes_coordinate_precision() {
        let left = MatchCacheLookupKey::new(
            MatchRequestKind::Waypoint,
            Some("FIX01"),
            Some(35.1234567841),
            Some(120.9876543249),
            true,
            Some("ZBAA"),
        );
        let right = MatchCacheLookupKey::new(
            MatchRequestKind::Waypoint,
            Some("FIX01"),
            Some(35.1234567844),
            Some(120.9876543246),
            true,
            Some("ZBAA"),
        );
        assert_eq!(left, right);
        assert_eq!(left.to_owned_key(), right.to_owned_key());
    }

    #[test]
    fn loads_existing_procedures_only_for_requested_airports() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let db_path =
            std::env::temp_dir().join(format!("pmdg_navdata_cli_proc_test_{}.db", unique));
        let db_path_str = db_path.to_string_lossy().into_owned();
        let conn = RustSqliteConnection::open_native(&db_path_str, 30).unwrap();
        conn.execute_statement_native(
            "CREATE TABLE tbl_test_proc (airport_identifier TEXT, procedure_identifier TEXT)",
            &[],
        )
        .unwrap();
        conn.execute_statement_native(
            "INSERT INTO tbl_test_proc (airport_identifier, procedure_identifier) VALUES (?, ?)",
            &[
                SqlValue::Text("ZBAA".to_string()),
                SqlValue::Text("PROC1".to_string()),
            ],
        )
        .unwrap();
        conn.execute_statement_native(
            "INSERT INTO tbl_test_proc (airport_identifier, procedure_identifier) VALUES (?, ?)",
            &[
                SqlValue::Text("ZSPD".to_string()),
                SqlValue::Text("PROC2".to_string()),
            ],
        )
        .unwrap();

        let map = load_existing_proc_map_from_conn(&conn, "tbl_test_proc", &[String::from("ZBAA")])
            .unwrap();

        conn.close_native();
        let _ = std::fs::remove_file(db_path);

        assert_eq!(map.len(), 1);
        assert!(map.contains_key("ZBAA"));
        assert!(!map.contains_key("ZSPD"));
    }

    #[test]
    fn prepare_existing_proc_map_excludes_zuls_rows_deleted_before_import() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let db_path =
            std::env::temp_dir().join(format!("pmdg_navdata_cli_proc_test_{}.db", unique));
        let db_path_str = db_path.to_string_lossy().into_owned();
        let conn = RustSqliteConnection::open_native(&db_path_str, 30).unwrap();

        conn.execute_statement_native(
            "CREATE TABLE tbl_sids (airport_identifier TEXT, procedure_identifier TEXT)",
            &[],
        )
        .unwrap();
        conn.execute_statement_native(
            "INSERT INTO tbl_sids (airport_identifier, procedure_identifier) VALUES (?, ?)",
            &[
                SqlValue::Text("ZULS".to_string()),
                SqlValue::Text("DEP01".to_string()),
            ],
        )
        .unwrap();
        conn.execute_statement_native(
            "INSERT INTO tbl_sids (airport_identifier, procedure_identifier) VALUES (?, ?)",
            &[
                SqlValue::Text("ZULS".to_string()),
                SqlValue::Text("KEEP".to_string()),
            ],
        )
        .unwrap();

        let map = prepare_existing_proc_map(&conn, "tbl_sids", &[String::from("ZULS")]).unwrap();

        conn.close_native();
        let _ = std::fs::remove_file(db_path);

        let zuls = map.get("ZULS").unwrap();
        assert!(zuls.contains(&Some("KEEP".to_string())));
        assert!(!zuls.contains(&Some("DEP01".to_string())));
    }

    #[test]
    fn deletes_zuls_special_sids() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let db_path =
            std::env::temp_dir().join(format!("pmdg_navdata_cli_proc_test_{}.db", unique));
        let db_path_str = db_path.to_string_lossy().into_owned();
        let conn = RustSqliteConnection::open_native(&db_path_str, 30).unwrap();

        conn.execute_statement_native(
            "CREATE TABLE tbl_sids (airport_identifier TEXT, procedure_identifier TEXT)",
            &[],
        )
        .unwrap();
        conn.execute_statement_native(
            "INSERT INTO tbl_sids (airport_identifier, procedure_identifier) VALUES (?, ?)",
            &[
                SqlValue::Text("ZULS".to_string()),
                SqlValue::Text("DEP01".to_string()),
            ],
        )
        .unwrap();
        conn.execute_statement_native(
            "INSERT INTO tbl_sids (airport_identifier, procedure_identifier) VALUES (?, ?)",
            &[
                SqlValue::Text("ZULS".to_string()),
                SqlValue::Text("EO02".to_string()),
            ],
        )
        .unwrap();
        conn.execute_statement_native(
            "INSERT INTO tbl_sids (airport_identifier, procedure_identifier) VALUES (?, ?)",
            &[
                SqlValue::Text("ZULS".to_string()),
                SqlValue::Text("KEEP".to_string()),
            ],
        )
        .unwrap();

        delete_zuls_special_procedures(&conn, "tbl_sids").unwrap();

        let mut remaining = Vec::new();
        conn.query_each_native(
            "SELECT airport_identifier, procedure_identifier FROM tbl_sids",
            &[],
            |row| {
                remaining.push((row.get::<_, String>(0)?, row.get::<_, String>(1)?));
                Ok(())
            },
        )
        .unwrap();

        assert!(remaining.contains(&("ZULS".to_string(), "KEEP".to_string())));
        assert!(!remaining.iter().any(|(a, p)| a == "ZULS" && p.starts_with("DEP")));
        assert!(!remaining.iter().any(|(a, p)| a == "ZULS" && p.starts_with("EO")));

        conn.close_native();
        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn deletes_zuls_special_stars() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let db_path =
            std::env::temp_dir().join(format!("pmdg_navdata_cli_proc_test_{}.db", unique));
        let db_path_str = db_path.to_string_lossy().into_owned();
        let conn = RustSqliteConnection::open_native(&db_path_str, 30).unwrap();

        conn.execute_statement_native(
            "CREATE TABLE tbl_stars (airport_identifier TEXT, procedure_identifier TEXT)",
            &[],
        )
        .unwrap();
        for proc in &[
            "DUM08A",
            "DUM09A",
            "DUM10A",
            "DUM28A",
            "IGD10A",
            "IGD28A",
            "IKU10A",
            "IKU28A",
        ] {
            conn.execute_statement_native(
                "INSERT INTO tbl_stars (airport_identifier, procedure_identifier) VALUES (?, ?)",
                &[
                    SqlValue::Text("ZULS".to_string()),
                    SqlValue::Text(proc.to_string()),
                ],
            )
            .unwrap();
        }
        conn.execute_statement_native(
            "INSERT INTO tbl_stars (airport_identifier, procedure_identifier) VALUES (?, ?)",
            &[
                SqlValue::Text("ZULS".to_string()),
                SqlValue::Text("KEEP".to_string()),
            ],
        )
        .unwrap();

        delete_zuls_special_procedures(&conn, "tbl_stars").unwrap();

        let mut remaining = Vec::new();
        conn.query_each_native(
            "SELECT airport_identifier, procedure_identifier FROM tbl_stars",
            &[],
            |row| {
                remaining.push((row.get::<_, String>(0)?, row.get::<_, String>(1)?));
                Ok(())
            },
        )
        .unwrap();

        assert!(remaining.contains(&("ZULS".to_string(), "KEEP".to_string())));
        assert!(!remaining
            .iter()
            .any(|(a, p)| a == "ZULS" && [
                "DUM08A",
                "DUM09A",
                "DUM10A",
                "DUM28A",
                "IGD10A",
                "IGD28A",
                "IKU10A",
                "IKU28A",
            ]
            .contains(&p.as_str())));

        conn.close_native();
        let _ = std::fs::remove_file(db_path);
    }

    #[test]
    fn deletes_zuls_special_iaps() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let db_path =
            std::env::temp_dir().join(format!("pmdg_navdata_cli_proc_test_{}.db", unique));
        let db_path_str = db_path.to_string_lossy().into_owned();
        let conn = RustSqliteConnection::open_native(&db_path_str, 30).unwrap();

        conn.execute_statement_native(
            "CREATE TABLE tbl_iaps (airport_identifier TEXT, procedure_identifier TEXT)",
            &[],
        )
        .unwrap();
        conn.execute_statement_native(
            "INSERT INTO tbl_iaps (airport_identifier, procedure_identifier) VALUES (?, ?)",
            &[
                SqlValue::Text("ZULS".to_string()),
                SqlValue::Text("R01".to_string()),
            ],
        )
        .unwrap();
        conn.execute_statement_native(
            "INSERT INTO tbl_iaps (airport_identifier, procedure_identifier) VALUES (?, ?)",
            &[
                SqlValue::Text("ZULS".to_string()),
                SqlValue::Text("KEEP".to_string()),
            ],
        )
        .unwrap();

        delete_zuls_special_procedures(&conn, "tbl_iaps").unwrap();

        let mut remaining = Vec::new();
        conn.query_each_native(
            "SELECT airport_identifier, procedure_identifier FROM tbl_iaps",
            &[],
            |row| {
                remaining.push((row.get::<_, String>(0)?, row.get::<_, String>(1)?));
                Ok(())
            },
        )
        .unwrap();

        assert!(remaining.contains(&("ZULS".to_string(), "KEEP".to_string())));
        assert!(!remaining
            .iter()
            .any(|(a, p)| a == "ZULS" && p.starts_with("R")));

        conn.close_native();
        let _ = std::fs::remove_file(db_path);
    }
}
