use anyhow::{anyhow, Result};
use std::fs::File;
use std::io::{BufRead, BufReader};

const DAT_READER_CAPACITY: usize = 256 * 1024;

fn parse_waypoint_type_decimal(code: &str) -> Option<String> {
    let value: u64 = code.parse().ok()?;
    let hex_code = format!("{value:x}");

    let len = hex_code.len();
    let g3 = if len >= 2 {
        &hex_code[len - 2..len]
    } else {
        &hex_code[..]
    };
    let g2 = if len >= 4 {
        &hex_code[len - 4..len - 2]
    } else {
        ""
    };
    let g1 = if len > 4 { &hex_code[..len - 4] } else { "00" };

    let groups = [g3, g2, g1];
    let mut out = String::new();
    for g in groups {
        if g.is_empty() {
            continue;
        }
        if g == "20" {
            out.push(' ');
            continue;
        }
        let byte = u8::from_str_radix(g, 16).ok()?;
        out.push(byte as char);
    }

    Some(out.trim().to_string())
}

fn is_supported_region(icao: &str) -> bool {
    matches!(
        icao,
        "VH" | "VM" | "ZB" | "ZS" | "ZH" | "ZG" | "ZY" | "ZU" | "ZW" | "ZL" | "ZJ" | "ZP" | "ZZ"
    )
}

fn parse_f64(input: &str) -> Option<f64> {
    input.parse().ok()
}

fn parse_nav_line_common(line: &str) -> Option<(Vec<&str>, &str)> {
    let parts: Vec<&str> = line.split_whitespace().collect();
    if parts.len() < 12 {
        return None;
    }

    let icao = parts[9];
    if !is_supported_region(icao) {
        return None;
    }

    Some((parts, icao))
}

fn parse_lat_lon(parts: &[&str]) -> Option<(f64, f64)> {
    Some((parse_f64(parts[1])?, parse_f64(parts[2])?))
}

type WaypointParsed = (bool, String, String, String, String, f64, f64);
type TerminalWaypointRow = (String, String, String, String, String, f64, f64);
type EnrouteWaypointRow = (String, String, String, f64, f64);
type VhfNavRow = (
    Option<String>,
    String,
    String,
    Option<String>,
    f64,
    String,
    f64,
    f64,
    f64,
    String,
    bool,
);
type NdbNavRow = (String, String, String, f64, f64, f64, f64);

pub struct CifpFields<'a> {
    line: &'a str,
    ranges: &'a [(usize, usize)],
}

impl<'a> CifpFields<'a> {
    pub fn get(&self, idx: usize) -> Option<&'a str> {
        self.ranges
            .get(idx)
            .map(|&(start, end)| &self.line[start..end])
    }

    pub fn first(&self) -> Option<&'a str> {
        self.get(0)
    }

    #[cfg(test)]
    pub fn iter(&self) -> impl Iterator<Item = &'a str> + '_ {
        self.ranges
            .iter()
            .map(|&(start, end)| &self.line[start..end])
    }
}

fn parse_waypoint_line(parts: &[&str]) -> Option<WaypointParsed> {
    if parts.len() < 6 {
        return None;
    }

    let region_code = parts[3].to_string();
    let icao_code = parts[4].to_string();
    if !is_supported_region(&icao_code) {
        return None;
    }

    let lat = parse_f64(parts[0])?;
    let lon = parse_f64(parts[1])?;
    let waypoint_type = parse_waypoint_type_decimal(parts[5])?;
    let waypoint_identifier = parts[2].to_string();
    let is_enroute = parts[3] == "ENRT";

    Some((
        is_enroute,
        region_code,
        icao_code,
        waypoint_identifier,
        waypoint_type,
        lat,
        lon,
    ))
}

fn trim_cifp_field_bounds(line: &str, mut start: usize, mut end: usize) -> (usize, usize) {
    let bytes = line.as_bytes();

    while start < end && bytes[start].is_ascii_whitespace() {
        start += 1;
    }
    while start < end && bytes[end - 1].is_ascii_whitespace() {
        end -= 1;
    }

    (start, end)
}

fn split_cifp_fields(
    line: &str,
    prefix: &str,
    min_fields: usize,
    fields: &mut Vec<(usize, usize)>,
) -> bool {
    let line = line.trim_end_matches(['\r', '\n']);
    if !line.starts_with(prefix) {
        return false;
    }

    fields.clear();
    let mut start = 0usize;
    while let Some(offset) = line[start..].find(',') {
        let end = start + offset;
        fields.push(trim_cifp_field_bounds(line, start, end));
        start = end + 1;
    }
    fields.push(trim_cifp_field_bounds(line, start, line.len()));

    if fields.len() < min_fields {
        fields.clear();
        return false;
    }

    true
}

pub fn for_each_cifp_line<R, F>(
    mut reader: R,
    prefix: &str,
    min_fields: usize,
    mut on_row: F,
) -> Result<()>
where
    R: BufRead,
    F: FnMut(CifpFields<'_>) -> Result<()>,
{
    let mut line = String::new();
    let field_capacity = min_fields.max(32);
    let mut fields = Vec::with_capacity(field_capacity);

    loop {
        line.clear();
        if reader.read_line(&mut line)? == 0 {
            break;
        }

        if !split_cifp_fields(&line, prefix, min_fields, &mut fields) {
            continue;
        }

        let parsed = CifpFields {
            line: &line,
            ranges: &fields,
        };
        on_row(parsed)?;
    }

    Ok(())
}

fn open_text_reader(file_path: &str) -> Result<BufReader<File>> {
    let file =
        File::open(file_path).map_err(|err| anyhow!("failed to open {file_path}: {err}"))?;
    Ok(BufReader::with_capacity(DAT_READER_CAPACITY, file))
}

fn collect_parsed_lines<R, T>(
    mut reader: R,
    mut parse_line: impl FnMut(&str, usize) -> Option<T>,
) -> Result<Vec<T>>
where
    R: BufRead,
{
    let mut out = Vec::new();
    let mut line = String::new();
    let mut line_number = 0usize;

    loop {
        line.clear();
        if reader.read_line(&mut line)? == 0 {
            break;
        }
        line_number += 1;
        let trimmed = line.trim_end_matches(['\r', '\n']);
        if let Some(parsed) = parse_line(trimmed, line_number) {
            out.push(parsed);
        }
    }

    Ok(out)
}

fn parse_terminal_waypoint_record(line: &str, line_number: usize) -> Option<TerminalWaypointRow> {
    if line_number <= 3 {
        return None;
    }

    let parts: Vec<&str> = line.split_whitespace().collect();
    let (is_enroute, region_code, icao_code, waypoint_identifier, waypoint_type, lat, lon) =
        parse_waypoint_line(&parts)?;

    if is_enroute {
        return None;
    }

    let waypoint_name = waypoint_identifier.clone();
    Some((
        region_code,
        icao_code,
        waypoint_identifier,
        waypoint_name,
        waypoint_type,
        lat,
        lon,
    ))
}

fn parse_enroute_waypoint_record(line: &str, line_number: usize) -> Option<EnrouteWaypointRow> {
    if line_number <= 3 {
        return None;
    }

    let parts: Vec<&str> = line.split_whitespace().collect();
    let (is_enroute, _region_code, icao_code, waypoint_identifier, waypoint_type, lat, lon) =
        parse_waypoint_line(&parts)?;

    if !is_enroute {
        return None;
    }

    Some((icao_code, waypoint_identifier, waypoint_type, lat, lon))
}

fn parse_vhf_nav_record(line: &str) -> Option<VhfNavRow> {
    let (parts, icao) = parse_nav_line_common(line)?;

    let nav_type = parts[parts.len() - 1];
    if !matches!(nav_type, "VOR/DME" | "DME-ILS") {
        return None;
    }

    if !matches!(parts[0], "3" | "12") {
        return None;
    }

    let (lat, lon) = parse_lat_lon(&parts)?;
    let dme_elevation = parse_f64(parts[3])?;

    let freq_raw = parts[4];
    let navaid_frequency: f64 = if freq_raw.len() >= 4 {
        let (head, tail) = freq_raw.split_at(3);
        format!("{head}.{tail}").parse().ok()?
    } else {
        return None;
    };

    let airport_identifier = if parts[8] == "ENRT" {
        None
    } else {
        Some(parts[8].to_string())
    };

    let navaid_identifier = parts[7].to_string();
    let navaid_name = parts
        .get(10)
        .map(|v| v.trim())
        .filter(|v| !v.is_empty())
        .map(std::string::ToString::to_string);

    let navaid_class = if nav_type == "VOR/DME" {
        "VDHW ".to_string()
    } else {
        " IT N".to_string()
    };

    let has_dme_ident = parts[0] == "12";

    Some((
        airport_identifier,
        icao.to_string(),
        navaid_identifier,
        navaid_name,
        navaid_frequency,
        navaid_class,
        lat,
        lon,
        dme_elevation,
        parts[5].to_string(),
        has_dme_ident,
    ))
}

fn parse_ndb_nav_record(line: &str) -> Option<NdbNavRow> {
    let (parts, icao) = parse_nav_line_common(line)?;

    if parts[8] != "ENRT" {
        return None;
    }
    if parts[parts.len() - 1] != "NDB" {
        return None;
    }

    let navaid_frequency = parse_f64(parts[4])?;
    let (navaid_latitude, navaid_longitude) = parse_lat_lon(&parts)?;
    let ndb_range = parse_f64(parts[5])?;

    Some((
        icao.to_string(),
        parts[7].to_string(),
        parts[10].to_string(),
        navaid_frequency,
        navaid_latitude,
        navaid_longitude,
        ndb_range,
    ))
}

pub fn parse_terminal_waypoints_file(file_path: &str) -> Result<Vec<TerminalWaypointRow>> {
    collect_parsed_lines(open_text_reader(file_path)?, parse_terminal_waypoint_record)
}

pub fn parse_enroute_waypoints_file(file_path: &str) -> Result<Vec<EnrouteWaypointRow>> {
    collect_parsed_lines(open_text_reader(file_path)?, parse_enroute_waypoint_record)
}

pub fn parse_vhf_nav_file(file_path: &str) -> Result<Vec<VhfNavRow>> {
    collect_parsed_lines(open_text_reader(file_path)?, |line, _| {
        parse_vhf_nav_record(line)
    })
}

pub fn parse_ndb_nav_file(file_path: &str) -> Result<Vec<NdbNavRow>> {
    collect_parsed_lines(open_text_reader(file_path)?, |line, _| {
        parse_ndb_nav_record(line)
    })
}

#[cfg(test)]
fn parse_terminal_waypoints_lines(content: &str) -> Vec<TerminalWaypointRow> {
    content
        .lines()
        .enumerate()
        .filter_map(|(idx, line)| parse_terminal_waypoint_record(line, idx + 1))
        .collect()
}

#[cfg(test)]
fn parse_enroute_waypoints_lines(content: &str) -> Vec<EnrouteWaypointRow> {
    content
        .lines()
        .enumerate()
        .filter_map(|(idx, line)| parse_enroute_waypoint_record(line, idx + 1))
        .collect()
}

#[cfg(test)]
fn parse_vhf_nav_lines(content: &str) -> Vec<VhfNavRow> {
    content.lines().filter_map(parse_vhf_nav_record).collect()
}

#[cfg(test)]
fn parse_ndb_nav_lines(content: &str) -> Vec<NdbNavRow> {
    content.lines().filter_map(parse_ndb_nav_record).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn parses_terminal_and_enroute_waypoints_separately() {
        let content = [
            "hdr1",
            "hdr2",
            "hdr3",
            "31.1000 121.2000 FIX01 TERM ZS 65",
            "32.1000 122.2000 FIX02 ENRT ZB 65",
            "33.1000 123.2000 FIX03 ENRT XX 65",
        ]
        .join("\n");

        let terminal = parse_terminal_waypoints_lines(&content);
        let enroute = parse_enroute_waypoints_lines(&content);

        assert_eq!(terminal.len(), 1);
        assert_eq!(terminal[0].0, "TERM");
        assert_eq!(terminal[0].1, "ZS");
        assert_eq!(terminal[0].2, "FIX01");
        assert!(terminal[0].4.starts_with('A'));

        assert_eq!(enroute.len(), 1);
        assert_eq!(enroute[0].0, "ZB");
        assert_eq!(enroute[0].1, "FIX02");
        assert!(enroute[0].2.starts_with('A'));
    }

    #[test]
    fn parses_vhf_line_with_shared_nav_precheck() {
        let content = "12 31.0 121.0 10 11320 50 X VOR1 ZSPD ZS NAVNAME DME-ILS";
        let rows = parse_vhf_nav_lines(content);

        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(row.0.as_deref(), Some("ZSPD"));
        assert_eq!(row.1, "ZS");
        assert_eq!(row.2, "VOR1");
        assert!((row.4 - 113.20).abs() < 1e-6);
        assert_eq!(row.5, " IT N");
        assert!(row.10);
    }

    #[test]
    fn parses_ndb_line_with_shared_nav_precheck() {
        let content = "2 30.0 120.0 0 375 25 X NDB1 ENRT ZS NDBNAME NDB";
        let rows = parse_ndb_nav_lines(content);

        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(row.0, "ZS");
        assert_eq!(row.1, "NDB1");
        assert_eq!(row.2, "NDBNAME");
        assert!((row.3 - 375.0).abs() < 1e-6);
        assert!((row.4 - 30.0).abs() < 1e-6);
        assert!((row.5 - 120.0).abs() < 1e-6);
        assert!((row.6 - 25.0).abs() < 1e-6);
    }

    #[test]
    fn streams_cifp_rows_with_reused_field_buffer() {
        let content = "SKIP,1\r\nAPP, A , PROC1 , FIX1\r\nAPP,B\r\nAPP, C, PROC2, FIX2\r\n";
        let mut rows = Vec::new();

        for_each_cifp_line(Cursor::new(content.as_bytes()), "APP", 4, |parts| {
            rows.push(parts.iter().map(str::to_string).collect::<Vec<_>>());
            Ok(())
        })
        .unwrap();

        assert_eq!(
            rows,
            vec![
                vec![
                    "APP".to_string(),
                    "A".to_string(),
                    "PROC1".to_string(),
                    "FIX1".to_string(),
                ],
                vec![
                    "APP".to_string(),
                    "C".to_string(),
                    "PROC2".to_string(),
                    "FIX2".to_string(),
                ],
            ]
        );
    }
}
