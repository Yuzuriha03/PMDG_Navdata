use crate::core::db::{
    get_shared_connection, open_sqlite_readonly_connection, RustSqliteConnection,
};
use crate::core::geo::haversine_m;
use anyhow::Result;
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;

static REF_MATCHER_CACHE: OnceLock<Mutex<HashMap<String, Arc<RefTableMatcher>>>> = OnceLock::new();
static COORDINATE_CACHE: OnceLock<Mutex<HashMap<String, Arc<SharedCoordinateCache>>>> =
    OnceLock::new();

const MATCHER_FILE_READER_CAPACITY: usize = 256 * 1024;
const SQLITE_MAX_VARIABLE_NUMBER: usize = 999;

const CODE_TYPE_DESIGNATED_POINT: &str = "DESIGNATED_POINT";
const CODE_TYPE_LOCAL_DESIGNATED_POINT: &str = "\u{5730}\u{540d}\u{70b9}";
const CODE_TYPE_VORDME: &str = "VORDME";
const CODE_TYPE_NDB: &str = "NDB";
const NAV_TYPE_VOR_DME: &str = "VOR/DME";

#[derive(Clone, Copy)]
pub(crate) struct RefMatchRequest<'a> {
    pub identifier: Option<&'a str>,
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
    pub is_airport: bool,
    pub airport_id: Option<&'a str>,
}

#[derive(Clone, Default)]
pub(crate) struct RefMatchResult {
    pub ref_table: Option<&'static str>,
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CoordinateSearchType {
    RecommendedNavaid,
    Waypoint,
    Center,
}

#[derive(Clone, Copy)]
pub(crate) struct CoordinateLookupRequest<'a> {
    pub search_type: CoordinateSearchType,
    pub identifier: Option<&'a str>,
    pub icao_code: Option<&'a str>,
    pub region_code: Option<&'a str>,
}

#[derive(Clone, Default)]
pub(crate) struct CoordinateLookupResult {
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
}

fn ref_matcher_cache() -> &'static Mutex<HashMap<String, Arc<RefTableMatcher>>> {
    REF_MATCHER_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn coordinate_cache() -> &'static Mutex<HashMap<String, Arc<SharedCoordinateCache>>> {
    COORDINATE_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn absolute_path(path: &str) -> Result<String> {
    if path.trim().is_empty() {
        return Ok(String::new());
    }

    let candidate = Path::new(path);
    let absolute = if candidate.is_absolute() {
        candidate.to_path_buf()
    } else {
        std::env::current_dir()?.join(candidate)
    };

    Ok(pathbuf_to_string(absolute))
}

fn absolute_optional_path(path: Option<&str>) -> Result<String> {
    match path {
        Some(value) if !value.is_empty() => absolute_path(value),
        _ => Ok(String::new()),
    }
}

fn pathbuf_to_string(path: PathBuf) -> String {
    path.to_string_lossy().into_owned()
}

fn experimental_parallel_db_matcher_enabled() -> bool {
    std::env::var_os("XP2PMDG_EXPERIMENTAL_PARALLEL_DB_MATCHER").is_some()
}

fn identifier_filter_fingerprint(required_identifiers: Option<&HashSet<Box<str>>>) -> String {
    match required_identifiers {
        None => "all".to_string(),
        Some(required_identifiers) => {
            let mut identifiers = required_identifiers
                .iter()
                .map(|identifier| identifier.as_ref())
                .collect::<Vec<_>>();
            identifiers.sort_unstable();

            let mut hasher = DefaultHasher::new();
            for identifier in identifiers {
                identifier.hash(&mut hasher);
            }

            format!(
                "scoped:{}:{:016x}",
                required_identifiers.len(),
                hasher.finish()
            )
        }
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

#[derive(Clone)]
struct RefCandidate {
    ref_table: &'static str,
    lat: f64,
    lon: f64,
    region: Option<String>,
    airport: Option<String>,
}

#[derive(Clone)]
pub(crate) struct CoordinateMatcher {
    fix_data: HashMap<String, HashMap<String, (f64, f64)>>,
    fix_data_by_region: HashMap<String, HashMap<String, HashMap<String, (f64, f64)>>>,
    fix_candidates_by_identifier: HashMap<String, HashMap<String, (f64, f64)>>,
    nav_data: HashMap<String, HashMap<String, (f64, f64)>>,
}

#[derive(Clone)]
pub(crate) struct SharedCoordinateCache {
    matcher: CoordinateMatcher,
}

#[derive(Clone)]
pub(crate) struct IcaoCodeResolver {
    fix_map: HashMap<String, String>,
    vor_dme_map: HashMap<String, String>,
    ndb_map: HashMap<String, String>,
}

#[derive(Clone)]
pub(crate) struct RefTableMatcher {
    airport_map: HashMap<String, (f64, f64)>,
    by_identifier: HashMap<String, Vec<RefCandidate>>,
}

fn push_candidate(
    by_identifier: &mut HashMap<String, Vec<RefCandidate>>,
    identifier: String,
    lat: f64,
    lon: f64,
    ref_table: &'static str,
    region: Option<String>,
    airport: Option<String>,
) {
    if identifier.is_empty() {
        return;
    }
    by_identifier
        .entry(identifier)
        .or_default()
        .push(RefCandidate {
            ref_table,
            lat,
            lon,
            region,
            airport,
        });
}

fn load_airport_rows_native(
    conn: &RustSqliteConnection,
    airport_map: &mut HashMap<String, (f64, f64)>,
) -> Result<()> {
    conn.query_each_native(
        "SELECT airport_identifier, airport_ref_latitude, airport_ref_longitude FROM tbl_airports",
        &[],
        |row| {
            let identifier: String = row.get(0)?;
            let Some(lat) = row.get::<_, Option<f64>>(1)? else {
                return Ok(());
            };
            let Some(lon) = row.get::<_, Option<f64>>(2)? else {
                return Ok(());
            };
            if identifier.is_empty() {
                return Ok(());
            }
            airport_map.entry(identifier).or_insert((lat, lon));
            Ok(())
        },
    )?;
    Ok(())
}

fn append_rows_3_native(
    conn: &RustSqliteConnection,
    sql: &str,
    by_identifier: &mut HashMap<String, Vec<RefCandidate>>,
    ref_table: &'static str,
) -> Result<()> {
    conn.query_each_native(sql, &[], |row| {
        let identifier: String = row.get(0)?;
        let Some(lat) = row.get::<_, Option<f64>>(1)? else {
            return Ok(());
        };
        let Some(lon) = row.get::<_, Option<f64>>(2)? else {
            return Ok(());
        };
        push_candidate(by_identifier, identifier, lat, lon, ref_table, None, None);
        Ok(())
    })?;
    Ok(())
}

fn load_airport_rows_scoped_native(
    conn: &RustSqliteConnection,
    airport_map: &mut HashMap<String, (f64, f64)>,
    required_identifiers: &HashSet<Box<str>>,
) -> Result<()> {
    if required_identifiers.is_empty() {
        return Ok(());
    }

    let identifiers = required_identifiers
        .iter()
        .map(|identifier| identifier.as_ref())
        .collect::<Vec<_>>();
    for chunk in identifiers.chunks(SQLITE_MAX_VARIABLE_NUMBER) {
        let placeholders = vec!["?"; chunk.len()].join(", ");
        let sql = format!(
            "SELECT airport_identifier, airport_ref_latitude, airport_ref_longitude FROM tbl_airports WHERE airport_identifier IN ({})",
            placeholders
        );
        let params = chunk
            .iter()
            .map(|identifier| rusqlite::types::Value::Text((*identifier).to_string()))
            .collect::<Vec<_>>();
        conn.query_each_native(&sql, &params, |row| {
            let identifier: String = row.get(0)?;
            let Some(lat) = row.get::<_, Option<f64>>(1)? else {
                return Ok(());
            };
            let Some(lon) = row.get::<_, Option<f64>>(2)? else {
                return Ok(());
            };
            airport_map.entry(identifier).or_insert((lat, lon));
            Ok(())
        })?;
    }

    Ok(())
}

fn append_rows_4_native(
    conn: &RustSqliteConnection,
    sql: &str,
    by_identifier: &mut HashMap<String, Vec<RefCandidate>>,
    ref_table: &'static str,
) -> Result<()> {
    conn.query_each_native(sql, &[], |row| {
        let identifier: String = row.get(0)?;
        let Some(lat) = row.get::<_, Option<f64>>(1)? else {
            return Ok(());
        };
        let Some(lon) = row.get::<_, Option<f64>>(2)? else {
            return Ok(());
        };
        let region: Option<String> = row.get(3)?;
        push_candidate(by_identifier, identifier, lat, lon, ref_table, region, None);
        Ok(())
    })?;
    Ok(())
}

fn append_rows_4_airport_native(
    conn: &RustSqliteConnection,
    sql: &str,
    by_identifier: &mut HashMap<String, Vec<RefCandidate>>,
    ref_table: &'static str,
) -> Result<()> {
    conn.query_each_native(sql, &[], |row| {
        let identifier: String = row.get(0)?;
        let Some(lat) = row.get::<_, Option<f64>>(1)? else {
            return Ok(());
        };
        let Some(lon) = row.get::<_, Option<f64>>(2)? else {
            return Ok(());
        };
        let airport: Option<String> = row.get(3)?;
        push_candidate(
            by_identifier,
            identifier,
            lat,
            lon,
            ref_table,
            None,
            airport,
        );
        Ok(())
    })?;
    Ok(())
}

fn append_rows_scoped_native(
    conn: &RustSqliteConnection,
    table_name: &str,
    _identifier_column: &str,
    _latitude_column: &str,
    _longitude_column: &str,
    region_column: Option<&str>,
    airport_column: Option<&str>,
    by_identifier: &mut HashMap<String, Vec<RefCandidate>>,
    ref_table: &'static str,
    required_identifiers: &HashSet<Box<str>>,
) -> Result<()> {
    if required_identifiers.is_empty() {
        return Ok(());
    }

    let identifiers = required_identifiers
        .iter()
        .map(|identifier| identifier.as_ref())
        .collect::<Vec<_>>();

    let select_prefix = match (table_name, region_column.is_some(), airport_column.is_some()) {
        ("tbl_enroute_ndbnavaids", false, false) => {
            "SELECT ndb_identifier, ndb_latitude, ndb_longitude FROM tbl_enroute_ndbnavaids WHERE ndb_identifier IN ({})"
        }
        ("tbl_vhfnavaids", false, false) => {
            "SELECT vor_identifier, vor_latitude, vor_longitude FROM tbl_vhfnavaids WHERE vor_identifier IN ({})"
        }
        ("tbl_terminal_ndbnavaids", false, true) => {
            "SELECT ndb_identifier, ndb_latitude, ndb_longitude, airport_identifier FROM tbl_terminal_ndbnavaids WHERE ndb_identifier IN ({})"
        }
        ("tbl_enroute_waypoints", false, false) => {
            "SELECT waypoint_identifier, waypoint_latitude, waypoint_longitude FROM tbl_enroute_waypoints WHERE waypoint_identifier IN ({})"
        }
        ("tbl_terminal_waypoints", true, false) => {
            "SELECT waypoint_identifier, waypoint_latitude, waypoint_longitude, region_code FROM tbl_terminal_waypoints WHERE waypoint_identifier IN ({})"
        }
        ("tbl_runways", false, true) => {
            "SELECT runway_identifier, runway_latitude, runway_longitude, airport_identifier FROM tbl_runways WHERE runway_identifier IN ({})"
        }
        ("tbl_localizers_glideslopes", false, true) => {
            "SELECT llz_identifier, llz_latitude, llz_longitude, airport_identifier FROM tbl_localizers_glideslopes WHERE llz_identifier IN ({})"
        }
        ("tbl_gls", false, true) => {
            "SELECT gls_ref_path_identifier, station_latitude, station_longitude, airport_identifier FROM tbl_gls WHERE gls_ref_path_identifier IN ({})"
        }
        _ => return Ok(()),
    };

    for chunk in identifiers.chunks(SQLITE_MAX_VARIABLE_NUMBER) {
        let placeholders = vec!["?"; chunk.len()].join(", ");
        let sql = select_prefix.replace("{}", &placeholders);
        let params = chunk
            .iter()
            .map(|identifier| rusqlite::types::Value::Text((*identifier).to_string()))
            .collect::<Vec<_>>();

        if region_column.is_some() || airport_column.is_some() {
            conn.query_each_native(&sql, &params, |row| {
                let identifier: String = row.get(0)?;
                let Some(lat) = row.get::<_, Option<f64>>(1)? else {
                    return Ok(());
                };
                let Some(lon) = row.get::<_, Option<f64>>(2)? else {
                    return Ok(());
                };
                let mut next_index = 3;
                let region = if region_column.is_some() {
                    let value: Option<String> = row.get(next_index)?;
                    next_index += 1;
                    value
                } else {
                    None
                };
                let airport = if airport_column.is_some() {
                    let value: Option<String> = row.get(next_index)?;
                    value
                } else {
                    None
                };
                push_candidate(
                    by_identifier,
                    identifier,
                    lat,
                    lon,
                    ref_table,
                    region,
                    airport,
                );
                Ok(())
            })?;
        } else {
            conn.query_each_native(&sql, &params, |row| {
                let identifier: String = row.get(0)?;
                let Some(lat) = row.get::<_, Option<f64>>(1)? else {
                    return Ok(());
                };
                let Some(lon) = row.get::<_, Option<f64>>(2)? else {
                    return Ok(());
                };
                push_candidate(by_identifier, identifier, lat, lon, ref_table, None, None);
                Ok(())
            })?;
        }
    }

    Ok(())
}

fn load_scoped_candidates_native(
    db_path: &str,
    timeout: u32,
    table_name: &'static str,
    _identifier_column: &'static str,
    _latitude_column: &'static str,
    _longitude_column: &'static str,
    region_column: Option<&'static str>,
    airport_column: Option<&'static str>,
    ref_table: &'static str,
    required_identifiers: Arc<HashSet<Box<str>>>,
) -> Result<Vec<(String, RefCandidate)>> {
    let conn = open_sqlite_readonly_connection(db_path, timeout)?;
    let mut rows = Vec::new();
    if !required_identifiers.is_empty() {
        let select_prefix = match (table_name, region_column.is_some(), airport_column.is_some()) {
            ("tbl_enroute_ndbnavaids", false, false) => {
                "SELECT ndb_identifier, ndb_latitude, ndb_longitude FROM tbl_enroute_ndbnavaids WHERE ndb_identifier IN ({})"
            }
            ("tbl_vhfnavaids", false, false) => {
                "SELECT vor_identifier, vor_latitude, vor_longitude FROM tbl_vhfnavaids WHERE vor_identifier IN ({})"
            }
            ("tbl_terminal_ndbnavaids", false, true) => {
                "SELECT ndb_identifier, ndb_latitude, ndb_longitude, airport_identifier FROM tbl_terminal_ndbnavaids WHERE ndb_identifier IN ({})"
            }
            ("tbl_enroute_waypoints", false, false) => {
                "SELECT waypoint_identifier, waypoint_latitude, waypoint_longitude FROM tbl_enroute_waypoints WHERE waypoint_identifier IN ({})"
            }
            ("tbl_terminal_waypoints", true, false) => {
                "SELECT waypoint_identifier, waypoint_latitude, waypoint_longitude, region_code FROM tbl_terminal_waypoints WHERE waypoint_identifier IN ({})"
            }
            ("tbl_runways", false, true) => {
                "SELECT runway_identifier, runway_latitude, runway_longitude, airport_identifier FROM tbl_runways WHERE runway_identifier IN ({})"
            }
            ("tbl_localizers_glideslopes", false, true) => {
                "SELECT llz_identifier, llz_latitude, llz_longitude, airport_identifier FROM tbl_localizers_glideslopes WHERE llz_identifier IN ({})"
            }
            ("tbl_gls", false, true) => {
                "SELECT gls_ref_path_identifier, station_latitude, station_longitude, airport_identifier FROM tbl_gls WHERE gls_ref_path_identifier IN ({})"
            }
            _ => return Ok(rows),
        };
        let identifiers = required_identifiers
            .iter()
            .map(|identifier| identifier.as_ref())
            .collect::<Vec<_>>();
        for chunk in identifiers.chunks(SQLITE_MAX_VARIABLE_NUMBER) {
            let placeholders = vec!["?"; chunk.len()].join(", ");
            let sql = select_prefix.replace("{}", &placeholders);
            let params = chunk
                .iter()
                .map(|identifier| rusqlite::types::Value::Text((*identifier).to_string()))
                .collect::<Vec<_>>();
            if region_column.is_some() || airport_column.is_some() {
                conn.query_each_native(&sql, &params, |row| {
                    let identifier: String = row.get(0)?;
                    let Some(lat) = row.get::<_, Option<f64>>(1)? else {
                        return Ok(());
                    };
                    let Some(lon) = row.get::<_, Option<f64>>(2)? else {
                        return Ok(());
                    };
                    let mut next_index = 3;
                    let region = if region_column.is_some() {
                        let value: Option<String> = row.get(next_index)?;
                        next_index += 1;
                        value
                    } else {
                        None
                    };
                    let airport = if airport_column.is_some() {
                        let value: Option<String> = row.get(next_index)?;
                        value
                    } else {
                        None
                    };
                    rows.push((
                        identifier,
                        RefCandidate {
                            ref_table,
                            lat,
                            lon,
                            region,
                            airport,
                        },
                    ));
                    Ok(())
                })?;
            } else {
                conn.query_each_native(&sql, &params, |row| {
                    let identifier: String = row.get(0)?;
                    let Some(lat) = row.get::<_, Option<f64>>(1)? else {
                        return Ok(());
                    };
                    let Some(lon) = row.get::<_, Option<f64>>(2)? else {
                        return Ok(());
                    };
                    rows.push((
                        identifier,
                        RefCandidate {
                            ref_table,
                            lat,
                            lon,
                            region: None,
                            airport: None,
                        },
                    ));
                    Ok(())
                })?;
            }
        }
    }
    conn.close_native();
    Ok(rows)
}

fn load_scoped_airports_native(
    db_path: &str,
    timeout: u32,
    required_identifiers: Arc<HashSet<Box<str>>>,
) -> Result<HashMap<String, (f64, f64)>> {
    let conn = open_sqlite_readonly_connection(db_path, timeout)?;
    let mut airport_map = HashMap::new();
    load_airport_rows_scoped_native(&conn, &mut airport_map, &required_identifiers)?;
    conn.close_native();
    Ok(airport_map)
}

fn create_ref_table_matcher_from_db_parallel(
    db_path: &str,
    timeout: u32,
    required_identifiers: Arc<HashSet<Box<str>>>,
) -> Result<RefTableMatcher> {
    let airport_required_identifiers = Arc::clone(&required_identifiers);
    let airport_path = db_path.to_owned();
    let airport_handle = thread::spawn(move || {
        load_scoped_airports_native(&airport_path, timeout, airport_required_identifiers)
    });

    let table_specs = [
        (
            "tbl_enroute_ndbnavaids",
            "ndb_identifier",
            "ndb_latitude",
            "ndb_longitude",
            None,
            None,
            "tbl_enroute_ndbnavaids",
        ),
        (
            "tbl_vhfnavaids",
            "vor_identifier",
            "vor_latitude",
            "vor_longitude",
            None,
            None,
            "tbl_vhfnavaids",
        ),
        (
            "tbl_terminal_ndbnavaids",
            "ndb_identifier",
            "ndb_latitude",
            "ndb_longitude",
            None,
            Some("airport_identifier"),
            "tbl_terminal_ndbnavaids",
        ),
        (
            "tbl_enroute_waypoints",
            "waypoint_identifier",
            "waypoint_latitude",
            "waypoint_longitude",
            None,
            None,
            "tbl_enroute_waypoints",
        ),
        (
            "tbl_terminal_waypoints",
            "waypoint_identifier",
            "waypoint_latitude",
            "waypoint_longitude",
            Some("region_code"),
            None,
            "tbl_terminal_waypoints",
        ),
        (
            "tbl_runways",
            "runway_identifier",
            "runway_latitude",
            "runway_longitude",
            None,
            Some("airport_identifier"),
            "tbl_runways",
        ),
        (
            "tbl_localizers_glideslopes",
            "llz_identifier",
            "llz_latitude",
            "llz_longitude",
            None,
            Some("airport_identifier"),
            "tbl_localizers_glideslopes",
        ),
        (
            "tbl_gls",
            "gls_ref_path_identifier",
            "station_latitude",
            "station_longitude",
            None,
            Some("airport_identifier"),
            "tbl_gls",
        ),
    ];

    let mut handles = Vec::with_capacity(table_specs.len());
    for spec in table_specs {
        let db_path = db_path.to_owned();
        let required_identifiers = Arc::clone(&required_identifiers);
        handles.push(thread::spawn(move || {
            load_scoped_candidates_native(
                &db_path,
                timeout,
                spec.0,
                spec.1,
                spec.2,
                spec.3,
                spec.4,
                spec.5,
                spec.6,
                required_identifiers,
            )
        }));
    }

    let airport_map = airport_handle
        .join()
        .map_err(|_| anyhow::anyhow!("parallel airport matcher worker panicked"))??;
    let mut by_identifier = HashMap::new();
    for handle in handles {
        for (identifier, candidate) in handle
            .join()
            .map_err(|_| anyhow::anyhow!("parallel DB matcher worker panicked"))??
        {
            by_identifier
                .entry(identifier)
                .or_insert_with(Vec::new)
                .push(candidate);
        }
    }
    Ok(RefTableMatcher {
        airport_map,
        by_identifier,
    })
}

fn push_fix_entry(
    fix_data: &mut HashMap<String, HashMap<String, (f64, f64)>>,
    fix_data_by_region: &mut HashMap<String, HashMap<String, HashMap<String, (f64, f64)>>>,
    fix_candidates_by_identifier: &mut HashMap<String, HashMap<String, (f64, f64)>>,
    identifier: String,
    icao_code: String,
    region_code: String,
    lat: f64,
    lon: f64,
) {
    if identifier.is_empty() || icao_code.is_empty() {
        return;
    }

    fix_data
        .entry(identifier.clone())
        .or_default()
        .insert(icao_code.clone(), (lat, lon));
    fix_data_by_region
        .entry(identifier.clone())
        .or_default()
        .entry(icao_code.clone())
        .or_default()
        .insert(region_code, (lat, lon));
    fix_candidates_by_identifier
        .entry(identifier)
        .or_default()
        .insert(icao_code, (lat, lon));
}

fn push_nav_entry(
    nav_data: &mut HashMap<String, HashMap<String, (f64, f64)>>,
    identifier: String,
    icao_code: String,
    lat: f64,
    lon: f64,
) {
    if identifier.is_empty() || icao_code.is_empty() {
        return;
    }

    nav_data
        .entry(identifier)
        .or_default()
        .insert(icao_code, (lat, lon));
}

impl RefTableMatcher {
    pub fn match_batch_native<'a, I>(&self, requests: I) -> Vec<RefMatchResult>
    where
        I: IntoIterator<Item = RefMatchRequest<'a>>,
    {
        let requests = requests.into_iter();
        let (lower_bound, _) = requests.size_hint();
        let mut out = Vec::with_capacity(lower_bound);

        for request in requests {
            let Some(identifier) = request.identifier else {
                out.push(RefMatchResult::default());
                continue;
            };

            if request.is_airport {
                if let Some((lat, lon)) = self.airport_map.get(identifier) {
                    out.push(RefMatchResult {
                        ref_table: Some("tbl_airports"),
                        latitude: Some(*lat),
                        longitude: Some(*lon),
                    });
                } else {
                    out.push(RefMatchResult::default());
                }
                continue;
            }

            let (Some(lat), Some(lon)) = (request.latitude, request.longitude) else {
                out.push(RefMatchResult::default());
                continue;
            };

            let Some(candidates) = self.by_identifier.get(identifier) else {
                out.push(RefMatchResult::default());
                continue;
            };

            let mut matched = None;
            for candidate in candidates {
                if (candidate.lat - lat).abs() >= 0.01 || (candidate.lon - lon).abs() >= 0.01 {
                    continue;
                }

                if let Some(airport_id) = request.airport_id {
                    if candidate.airport.as_deref().is_some()
                        && candidate.airport.as_deref() != Some(airport_id)
                    {
                        continue;
                    }
                }

                if candidate.ref_table == "tbl_terminal_waypoints" {
                    if let Some(airport_id) = request.airport_id {
                        if candidate.region.as_deref() != Some(airport_id) {
                            continue;
                        }
                    }
                }

                if haversine_m(lat, lon, candidate.lat, candidate.lon) < 1000.0 {
                    matched = Some(RefMatchResult {
                        ref_table: Some(candidate.ref_table),
                        latitude: Some(candidate.lat),
                        longitude: Some(candidate.lon),
                    });
                    break;
                }
            }

            out.push(matched.unwrap_or_default());
        }

        out
    }
}

impl CoordinateMatcher {
    fn new() -> Self {
        Self {
            fix_data: HashMap::new(),
            fix_data_by_region: HashMap::new(),
            fix_candidates_by_identifier: HashMap::new(),
            nav_data: HashMap::new(),
        }
    }

    pub fn find_coordinates_native(
        &self,
        request: &CoordinateLookupRequest<'_>,
    ) -> (Option<f64>, Option<f64>) {
        let Some(identifier) = request.identifier else {
            return (None, None);
        };
        let Some(icao_code) = request.icao_code else {
            return (None, None);
        };

        match request.search_type {
            CoordinateSearchType::RecommendedNavaid => {
                if let Some((lat, lon)) = self
                    .nav_data
                    .get(identifier)
                    .and_then(|by_icao| by_icao.get(icao_code))
                {
                    return (Some(*lat), Some(*lon));
                }
                return (None, None);
            }
            CoordinateSearchType::Waypoint | CoordinateSearchType::Center => {}
        }

        let has_digit = identifier.chars().any(|c| c.is_ascii_digit());
        let id_len = identifier.chars().count();

        if has_digit || id_len == 4 || id_len == 5 {
            if let Some(region) = request.region_code {
                if let Some((lat, lon)) = self
                    .fix_data_by_region
                    .get(identifier)
                    .and_then(|by_icao| by_icao.get(icao_code))
                    .and_then(|by_region| by_region.get(region))
                {
                    return (Some(*lat), Some(*lon));
                }
            }

            if let Some((lat, lon)) = self
                .fix_data
                .get(identifier)
                .and_then(|by_icao| by_icao.get(icao_code))
            {
                return (Some(*lat), Some(*lon));
            }

            if let Some((lat, lon)) = self.find_waypoint_fallback(identifier) {
                return (Some(lat), Some(lon));
            }

            return (None, None);
        }

        if id_len == 1 || id_len == 2 || id_len == 3 {
            if let Some((lat, lon)) = self
                .nav_data
                .get(identifier)
                .and_then(|by_icao| by_icao.get(icao_code))
            {
                return (Some(*lat), Some(*lon));
            }
        }

        (None, None)
    }

    fn find_waypoint_fallback(&self, identifier: &str) -> Option<(f64, f64)> {
        let candidates = self.fix_candidates_by_identifier.get(identifier)?;

        if let Some(point) = candidates.get("ZZ") {
            return Some(*point);
        }

        let mut unique_point: Option<(f64, f64)> = None;
        for point in candidates.values() {
            match unique_point {
                None => unique_point = Some(*point),
                Some(existing) if existing != *point => return None,
                Some(_) => {}
            }
        }
        unique_point
    }
}

impl SharedCoordinateCache {
    pub fn find_coordinates_native(
        &self,
        request: CoordinateLookupRequest<'_>,
    ) -> CoordinateLookupResult {
        let (lat, lon) = self.matcher.find_coordinates_native(&request);
        match (lat, lon) {
            (Some(lat), Some(lon)) => CoordinateLookupResult {
                latitude: Some(lat),
                longitude: Some(lon),
            },
            _ => CoordinateLookupResult::default(),
        }
    }
}

impl IcaoCodeResolver {
    pub fn from_items(
        fix_items: Vec<(String, String)>,
        nav_items: Vec<(String, String, String)>,
    ) -> Self {
        let mut fix_map = HashMap::new();
        for (identifier, icao) in fix_items {
            if !identifier.is_empty() && !icao.is_empty() {
                fix_map.insert(identifier, icao);
            }
        }

        let mut vor_dme_map = HashMap::new();
        let mut ndb_map = HashMap::new();
        for (identifier, nav_type, icao) in nav_items {
            if !identifier.is_empty() && !nav_type.is_empty() && !icao.is_empty() {
                match nav_type.as_str() {
                    NAV_TYPE_VOR_DME => {
                        vor_dme_map.insert(identifier, icao);
                    }
                    CODE_TYPE_NDB => {
                        ndb_map.insert(identifier, icao);
                    }
                    _ => {}
                }
            }
        }

        Self {
            fix_map,
            vor_dme_map,
            ndb_map,
        }
    }

    pub fn resolve_ref(
        &self,
        waypoint_identifier: Option<&str>,
        code_type: Option<&str>,
    ) -> Option<String> {
        let identifier = waypoint_identifier?;
        let code_type = code_type?;

        match code_type {
            CODE_TYPE_DESIGNATED_POINT | CODE_TYPE_LOCAL_DESIGNATED_POINT => {
                self.fix_map.get(identifier).cloned()
            }
            CODE_TYPE_VORDME => self.vor_dme_map.get(identifier).cloned(),
            CODE_TYPE_NDB => self.ndb_map.get(identifier).cloned(),
            _ => None,
        }
    }
}

pub(crate) fn create_coordinate_matcher_from_files(
    earth_fix_path: Option<String>,
    earth_nav_path: Option<String>,
    required_identifiers: Option<&HashSet<Box<str>>>,
) -> CoordinateMatcher {
    let mut matcher = CoordinateMatcher::new();
    let mut seen_nav_keys = HashSet::new();

    if let Some(path) = earth_fix_path {
        if !path.is_empty() {
            if let Ok(file) = File::open(path) {
                let mut reader = BufReader::with_capacity(MATCHER_FILE_READER_CAPACITY, file);
                let mut line = String::new();
                loop {
                    line.clear();
                    let Ok(read) = reader.read_line(&mut line) else {
                        break;
                    };
                    if read == 0 {
                        break;
                    }

                    let mut cursor = 0usize;
                    let Some(lat_field) = next_ascii_field(&line, &mut cursor) else {
                        continue;
                    };
                    let Some(lon_field) = next_ascii_field(&line, &mut cursor) else {
                        continue;
                    };
                    let Some(identifier) = next_ascii_field(&line, &mut cursor) else {
                        continue;
                    };
                    if required_identifiers.is_some_and(|required| !required.contains(identifier)) {
                        continue;
                    }
                    let Some(region_code) = next_ascii_field(&line, &mut cursor) else {
                        continue;
                    };
                    let Some(icao_code) = next_ascii_field(&line, &mut cursor) else {
                        continue;
                    };

                    let lat: f64 = match lat_field.parse() {
                        Ok(value) => value,
                        Err(_) => continue,
                    };
                    let lon: f64 = match lon_field.parse() {
                        Ok(value) => value,
                        Err(_) => continue,
                    };
                    push_fix_entry(
                        &mut matcher.fix_data,
                        &mut matcher.fix_data_by_region,
                        &mut matcher.fix_candidates_by_identifier,
                        identifier.to_string(),
                        icao_code.to_string(),
                        region_code.to_string(),
                        lat,
                        lon,
                    );
                }
            }
        }
    }

    if let Some(path) = earth_nav_path {
        if !path.is_empty() {
            if let Ok(file) = File::open(path) {
                let mut reader = BufReader::with_capacity(MATCHER_FILE_READER_CAPACITY, file);
                let mut line = String::new();
                loop {
                    line.clear();
                    let Ok(read) = reader.read_line(&mut line) else {
                        break;
                    };
                    if read == 0 {
                        break;
                    }

                    let mut cursor = 0usize;
                    let Some(_record_type) = next_ascii_field(&line, &mut cursor) else {
                        continue;
                    };
                    let Some(lat_field) = next_ascii_field(&line, &mut cursor) else {
                        continue;
                    };
                    let Some(lon_field) = next_ascii_field(&line, &mut cursor) else {
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
                    let Some(identifier_6) = next_ascii_field(&line, &mut cursor) else {
                        continue;
                    };
                    let Some(identifier_7) = next_ascii_field(&line, &mut cursor) else {
                        continue;
                    };
                    let Some(identifier_8) = next_ascii_field(&line, &mut cursor) else {
                        continue;
                    };
                    let Some(icao_code_9) = next_ascii_field(&line, &mut cursor) else {
                        continue;
                    };
                    let Some(icao_code_10) = next_ascii_field(&line, &mut cursor) else {
                        continue;
                    };

                    let needs_identifier_6 =
                        required_identifiers.is_none_or(|required| required.contains(identifier_6));
                    let needs_identifier_7 =
                        required_identifiers.is_none_or(|required| required.contains(identifier_7));
                    let needs_identifier_8 =
                        required_identifiers.is_none_or(|required| required.contains(identifier_8));
                    if !needs_identifier_6 && !needs_identifier_7 && !needs_identifier_8 {
                        continue;
                    }

                    let lat: f64 = match lat_field.parse() {
                        Ok(value) => value,
                        Err(_) => continue,
                    };
                    let lon: f64 = match lon_field.parse() {
                        Ok(value) => value,
                        Err(_) => continue,
                    };

                    if needs_identifier_8 {
                        let identifier = identifier_8.to_string();
                        let icao_code = icao_code_10.to_string();
                        let key = (identifier.clone(), icao_code.clone());
                        if seen_nav_keys.insert(key) {
                            push_nav_entry(&mut matcher.nav_data, identifier, icao_code, lat, lon);
                        }
                    }

                    if needs_identifier_7 {
                        let identifier = identifier_7.to_string();
                        let icao_code = icao_code_9.to_string();
                        let key = (identifier.clone(), icao_code.clone());
                        if seen_nav_keys.insert(key) {
                            push_nav_entry(&mut matcher.nav_data, identifier, icao_code, lat, lon);
                        }
                    }

                    if needs_identifier_6 {
                        let identifier = identifier_6.to_string();
                        let icao_code = identifier_8.to_string();
                        let key = (identifier.clone(), icao_code.clone());
                        if seen_nav_keys.insert(key) {
                            push_nav_entry(&mut matcher.nav_data, identifier, icao_code, lat, lon);
                        }
                    }
                }
            }
        }
    }

    matcher
}

pub(crate) fn get_shared_coordinate_cache(
    earth_fix_path: Option<String>,
    earth_nav_path: Option<String>,
    required_identifiers: Option<Arc<HashSet<Box<str>>>>,
) -> Result<Arc<SharedCoordinateCache>> {
    let key = format!(
        "{}|{}|{}",
        absolute_optional_path(earth_fix_path.as_deref())?,
        absolute_optional_path(earth_nav_path.as_deref())?,
        identifier_filter_fingerprint(required_identifiers.as_deref())
    );

    if let Some(cached) = coordinate_cache().lock().unwrap().get(&key).cloned() {
        return Ok(cached);
    }

    let cache = Arc::new(SharedCoordinateCache {
        matcher: create_coordinate_matcher_from_files(
            earth_fix_path,
            earth_nav_path,
            required_identifiers.as_deref(),
        ),
    });
    coordinate_cache()
        .lock()
        .unwrap()
        .insert(key, Arc::clone(&cache));
    Ok(cache)
}

pub(crate) fn create_ref_table_matcher_from_db(
    conn: &RustSqliteConnection,
    required_identifiers: Option<&HashSet<Box<str>>>,
) -> Result<RefTableMatcher> {
    let mut airport_map = HashMap::new();
    let mut by_identifier = HashMap::new();

    if let Some(required_identifiers) = required_identifiers {
        load_airport_rows_scoped_native(conn, &mut airport_map, required_identifiers)?;
        append_rows_scoped_native(
            conn,
            "tbl_enroute_ndbnavaids",
            "ndb_identifier",
            "ndb_latitude",
            "ndb_longitude",
            None,
            None,
            &mut by_identifier,
            "tbl_enroute_ndbnavaids",
            required_identifiers,
        )?;
        append_rows_scoped_native(
            conn,
            "tbl_vhfnavaids",
            "vor_identifier",
            "vor_latitude",
            "vor_longitude",
            None,
            None,
            &mut by_identifier,
            "tbl_vhfnavaids",
            required_identifiers,
        )?;
        append_rows_scoped_native(
            conn,
            "tbl_terminal_ndbnavaids",
            "ndb_identifier",
            "ndb_latitude",
            "ndb_longitude",
            None,
            Some("airport_identifier"),
            &mut by_identifier,
            "tbl_terminal_ndbnavaids",
            required_identifiers,
        )?;
        append_rows_scoped_native(
            conn,
            "tbl_enroute_waypoints",
            "waypoint_identifier",
            "waypoint_latitude",
            "waypoint_longitude",
            None,
            None,
            &mut by_identifier,
            "tbl_enroute_waypoints",
            required_identifiers,
        )?;
        append_rows_scoped_native(
            conn,
            "tbl_terminal_waypoints",
            "waypoint_identifier",
            "waypoint_latitude",
            "waypoint_longitude",
            Some("region_code"),
            None,
            &mut by_identifier,
            "tbl_terminal_waypoints",
            required_identifiers,
        )?;
        append_rows_scoped_native(
            conn,
            "tbl_runways",
            "runway_identifier",
            "runway_latitude",
            "runway_longitude",
            None,
            Some("airport_identifier"),
            &mut by_identifier,
            "tbl_runways",
            required_identifiers,
        )?;
        append_rows_scoped_native(
            conn,
            "tbl_localizers_glideslopes",
            "llz_identifier",
            "llz_latitude",
            "llz_longitude",
            None,
            Some("airport_identifier"),
            &mut by_identifier,
            "tbl_localizers_glideslopes",
            required_identifiers,
        )?;
        append_rows_scoped_native(
            conn,
            "tbl_gls",
            "gls_ref_path_identifier",
            "station_latitude",
            "station_longitude",
            None,
            Some("airport_identifier"),
            &mut by_identifier,
            "tbl_gls",
            required_identifiers,
        )?;
    } else {
        load_airport_rows_native(conn, &mut airport_map)?;
        append_rows_3_native(
            conn,
            "SELECT ndb_identifier, ndb_latitude, ndb_longitude FROM tbl_enroute_ndbnavaids",
            &mut by_identifier,
            "tbl_enroute_ndbnavaids",
        )?;
        append_rows_3_native(
            conn,
            "SELECT vor_identifier, vor_latitude, vor_longitude FROM tbl_vhfnavaids",
            &mut by_identifier,
            "tbl_vhfnavaids",
        )?;
        append_rows_3_native(
            conn,
            "SELECT ndb_identifier, ndb_latitude, ndb_longitude FROM tbl_terminal_ndbnavaids",
            &mut by_identifier,
            "tbl_terminal_ndbnavaids",
        )?;
        append_rows_3_native(
            conn,
            "SELECT waypoint_identifier, waypoint_latitude, waypoint_longitude FROM tbl_enroute_waypoints",
            &mut by_identifier,
            "tbl_enroute_waypoints",
        )?;
        append_rows_4_native(
            conn,
            "SELECT waypoint_identifier, waypoint_latitude, waypoint_longitude, region_code FROM tbl_terminal_waypoints",
            &mut by_identifier,
            "tbl_terminal_waypoints",
        )?;
        append_rows_4_airport_native(
            conn,
            "SELECT ndb_identifier, ndb_latitude, ndb_longitude, airport_identifier FROM tbl_terminal_ndbnavaids",
            &mut by_identifier,
            "tbl_terminal_ndbnavaids",
        )?;
        append_rows_4_airport_native(
            conn,
            "SELECT runway_identifier, runway_latitude, runway_longitude, airport_identifier FROM tbl_runways",
            &mut by_identifier,
            "tbl_runways",
        )?;
        append_rows_4_airport_native(
            conn,
            "SELECT llz_identifier, llz_latitude, llz_longitude, airport_identifier FROM tbl_localizers_glideslopes",
            &mut by_identifier,
            "tbl_localizers_glideslopes",
        )?;
        append_rows_4_airport_native(
            conn,
            "SELECT gls_ref_path_identifier, station_latitude, station_longitude, airport_identifier FROM tbl_gls",
            &mut by_identifier,
            "tbl_gls",
        )?;
    }

    Ok(RefTableMatcher {
        airport_map,
        by_identifier,
    })
}

pub(crate) fn get_shared_ref_matcher(
    db_path: &str,
    timeout: u32,
    required_identifiers: Option<Arc<HashSet<Box<str>>>>,
) -> Result<Arc<RefTableMatcher>> {
    let key = format!(
        "{}|{}",
        absolute_path(db_path)?,
        identifier_filter_fingerprint(required_identifiers.as_deref())
    );
    if let Some(cached) = ref_matcher_cache().lock().unwrap().get(&key).cloned() {
        return Ok(cached);
    }

    let matcher = if let Some(conn) = get_shared_connection(db_path)? {
        if experimental_parallel_db_matcher_enabled() {
            if let Some(required_identifiers) = required_identifiers.clone() {
                create_ref_table_matcher_from_db_parallel(db_path, timeout, required_identifiers)?
            } else {
                create_ref_table_matcher_from_db(&conn, required_identifiers.as_deref())?
            }
        } else {
            create_ref_table_matcher_from_db(&conn, required_identifiers.as_deref())?
        }
    } else {
        if experimental_parallel_db_matcher_enabled() {
            if let Some(required_identifiers) = required_identifiers.clone() {
                create_ref_table_matcher_from_db_parallel(db_path, timeout, required_identifiers)?
            } else {
                let conn = RustSqliteConnection::open_native(db_path, timeout)?;
                let matcher =
                    create_ref_table_matcher_from_db(&conn, required_identifiers.as_deref())?;
                conn.close_native();
                matcher
            }
        } else {
            let conn = RustSqliteConnection::open_native(db_path, timeout)?;
            let matcher = create_ref_table_matcher_from_db(&conn, required_identifiers.as_deref())?;
            conn.close_native();
            matcher
        }
    };

    let matcher = Arc::new(matcher);
    ref_matcher_cache()
        .lock()
        .unwrap()
        .insert(key, Arc::clone(&matcher));
    Ok(matcher)
}
