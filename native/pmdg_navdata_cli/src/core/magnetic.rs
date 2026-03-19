use anyhow::{anyhow, Context, Result};
use num_traits::ToPrimitive;
use rayon::prelude::*;
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::OnceLock;

const WMM_STANDARD: &str = include_str!("magcof/WMM.COF");
const WMM_HIGH_RESOLUTION: &str = include_str!("magcof/WMMHR.COF");

const WMM_SIZE_STANDARD: usize = 12;
const WMM_SIZE_HIGH_RESOLUTION: usize = 133;
const WMM_MAX_SIZE: usize = 134;
const WMM_FLAT_SIZE: usize = WMM_MAX_SIZE * WMM_MAX_SIZE;
const MAGNETIC_PARALLEL_THRESHOLD: usize = 64;

const BLACKOUT_ZONE: f64 = 2000.0;
const CAUTION_ZONE: f64 = 6000.0;

const WGS84_A: f64 = 6378.137;
const WGS84_B: f64 = 6_356.752_314_2;
const WGS84_RE: f64 = 6371.2;

#[derive(Clone)]
struct GeoMag {
    maxord: usize,
    size: usize,
    epoch: f64,
    c: Box<[f64]>,
    cd: Box<[f64]>,
    k: Box<[f64]>,
    fn_values: [f64; WMM_MAX_SIZE],
    fm_values: [f64; WMM_MAX_SIZE],
}

#[derive(Default, Clone, Copy)]
struct GeoMagResult {
    d: f64,
    h: f64,
}

struct GeoMagFrame {
    ct: f64,
    st: f64,
    ca: f64,
    sa: f64,
    aor: f64,
}

struct GeoMagScratch {
    dp: Box<[f64]>,
    sp: [f64; WMM_MAX_SIZE],
    cp: [f64; WMM_MAX_SIZE],
    pp: [f64; WMM_MAX_SIZE],
    p: Box<[f64]>,
}

impl GeoMagScratch {
    fn new() -> Self {
        Self {
            dp: vec![0.0; WMM_FLAT_SIZE].into_boxed_slice(),
            sp: [0.0; WMM_MAX_SIZE],
            cp: [0.0; WMM_MAX_SIZE],
            pp: [0.0; WMM_MAX_SIZE],
            p: vec![0.0; WMM_FLAT_SIZE].into_boxed_slice(),
        }
    }

    fn prepare(&mut self) {
        self.sp[0] = 0.0;
        self.cp[0] = 1.0;
        self.pp[0] = 1.0;
        self.p[0] = 1.0;
        self.dp[0] = 0.0;
    }
}

struct ThreadLocalGeoMagState {
    scratch: GeoMagScratch,
    cache: HashMap<(u64, u64), f64>,
}

impl ThreadLocalGeoMagState {
    fn new() -> Self {
        Self {
            scratch: GeoMagScratch::new(),
            cache: HashMap::new(),
        }
    }
}

impl GeoMag {
    #[inline]
    const fn coeff_idx(m: usize, n: usize) -> usize {
        m * WMM_MAX_SIZE + n
    }

    #[inline]
    const fn p_idx(&self, n: usize, m: usize) -> usize {
        n + m * self.size
    }

    fn new(high_resolution: bool, coefficients_text: &str) -> Result<Self> {
        let maxord = if high_resolution {
            WMM_SIZE_HIGH_RESOLUTION
        } else {
            WMM_SIZE_STANDARD
        };
        let mut model = Self {
            maxord,
            size: maxord + 1,
            epoch: 0.0,
            c: vec![0.0; WMM_FLAT_SIZE].into_boxed_slice(),
            cd: vec![0.0; WMM_FLAT_SIZE].into_boxed_slice(),
            k: vec![0.0; WMM_FLAT_SIZE].into_boxed_slice(),
            fn_values: [0.0; WMM_MAX_SIZE],
            fm_values: [0.0; WMM_MAX_SIZE],
        };
        model.parse_coefficients_buffer(coefficients_text)?;
        Ok(model)
    }

    fn parse_coefficients_buffer(&mut self, text: &str) -> Result<()> {
        let mut lines = text.lines();
        let header = lines
            .next()
            .ok_or_else(|| anyhow!("coefficient file is empty"))?;
        let header_parts: Vec<&str> = header.split_whitespace().collect();
        if header_parts.len() < 3 {
            return Err(anyhow!("invalid coefficient header"));
        }
        self.epoch = header_parts[0]
            .parse::<f64>()
            .with_context(|| format!("invalid epoch in header: {}", header_parts[0]))?;

        self.c.fill(0.0);
        self.cd.fill(0.0);
        self.k.fill(0.0);
        self.c[Self::coeff_idx(0, 0)] = 0.0;
        self.cd[Self::coeff_idx(0, 0)] = 0.0;

        for line in lines {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() < 6 {
                continue;
            }
            let n = parts[0]
                .parse::<usize>()
                .with_context(|| format!("invalid n: {}", parts[0]))?;
            if n == 9999 {
                break;
            }
            let m = parts[1]
                .parse::<usize>()
                .with_context(|| format!("invalid m: {}", parts[1]))?;
            let gnm = parts[2]
                .parse::<f64>()
                .with_context(|| format!("invalid gnm: {}", parts[2]))?;
            let hnm = parts[3]
                .parse::<f64>()
                .with_context(|| format!("invalid hnm: {}", parts[3]))?;
            let secular_primary_coeff = parts[4]
                .parse::<f64>()
                .with_context(|| format!("invalid dgnm: {}", parts[4]))?;
            let secular_secondary_coeff = parts[5]
                .parse::<f64>()
                .with_context(|| format!("invalid dhnm: {}", parts[5]))?;

            if m > self.maxord {
                break;
            }
            if m > n || n >= self.size || m >= self.size {
                return Err(anyhow!("corrupt coefficient record (n={n}, m={m})"));
            }

            self.c[Self::coeff_idx(m, n)] = gnm;
            self.cd[Self::coeff_idx(m, n)] = secular_primary_coeff;
            if m != 0 {
                self.c[Self::coeff_idx(n, m - 1)] = hnm;
                self.cd[Self::coeff_idx(n, m - 1)] = secular_secondary_coeff;
            }
        }

        self.finalize_coefficients();
        Ok(())
    }

    fn finalize_coefficients(&mut self) {
        let mut snorm = vec![0.0_f64; WMM_FLAT_SIZE].into_boxed_slice();
        snorm[0] = 1.0;
        self.fm_values[0] = 0.0;

        for n in 1..=self.maxord {
            let numerator = (2 * n - 1).to_f64().unwrap_or(f64::MAX);
            let denominator = n.to_f64().unwrap_or(1.0);
            snorm[n] = snorm[n - 1] * numerator / denominator;
            let mut j = 2_u32;
            for m in 0..=n {
                let n_f = n.to_f64().unwrap_or(f64::MAX);
                let m_f = m.to_f64().unwrap_or(f64::MAX);
                self.k[Self::coeff_idx(m, n)] =
                    m_f.mul_add(-m_f, (n_f - 1.0).powi(2)) / (2.0f64.mul_add(n_f, -1.0) * 2.0f64.mul_add(n_f, -3.0));
                if m > 0 {
                    let j_usize = usize::try_from(j).unwrap_or(usize::MAX);
                    let flnmj = ((n - m + 1) * j_usize).to_f64().unwrap_or(f64::MAX)
                        / (n + m).to_f64().unwrap_or(1.0);
                    let idx = self.p_idx(n, m);
                    let prev_idx = self.p_idx(n, m - 1);
                    snorm[idx] = snorm[prev_idx] * flnmj.sqrt();
                    j = 1;
                    self.c[Self::coeff_idx(n, m - 1)] *= snorm[idx];
                    self.cd[Self::coeff_idx(n, m - 1)] *= snorm[idx];
                }
                let idx = self.p_idx(n, m);
                self.c[Self::coeff_idx(m, n)] *= snorm[idx];
                self.cd[Self::coeff_idx(m, n)] *= snorm[idx];
            }
            self.fn_values[n] = (n + 1).to_f64().unwrap_or(f64::MAX);
            self.fm_values[n] = n.to_f64().unwrap_or(f64::MAX);
        }

        self.k[Self::coeff_idx(1, 1)] = 0.0;
    }

    fn initialize_frame(
        &self,
        glat: f64,
        glon: f64,
        alt: f64,
        sp: &mut [f64],
        cp: &mut [f64],
    ) -> GeoMagFrame {
        let semi_major_axis = WGS84_A;
        let semi_minor_axis = WGS84_B;
        let re = WGS84_RE;
        let a2 = semi_major_axis * semi_major_axis;
        let b2 = semi_minor_axis * semi_minor_axis;
        let c2 = a2 - b2;
        let a4 = a2 * a2;
        let b4 = b2 * b2;
        let c4 = a4 - b4;

        let rlon = glon.to_radians();
        let rlat = glat.to_radians();
        let srlon = rlon.sin();
        let srlat = rlat.sin();
        let crlon = rlon.cos();
        let crlat = rlat.cos();
        let srlat2 = srlat * srlat;
        let crlat2 = crlat * crlat;

        sp[1] = srlon;
        cp[1] = crlon;

        let geocentric_q = c2.mul_add(-srlat2, a2).sqrt();
        let q1 = alt * geocentric_q;
        let ratio = (q1 + a2) / (q1 + b2);
        let q2 = ratio * ratio;
        let ct = srlat / q2.mul_add(crlat2, srlat2).sqrt();
        let st = ct.mul_add(-ct, 1.0).sqrt();
        let r2 = alt.mul_add(alt, 2.0 * q1) + c4.mul_add(-srlat2, a4) / (geocentric_q * geocentric_q);
        let earth_radius = r2.sqrt();
        let surface_radius = a2.mul_add(crlat2, b2 * srlat2).sqrt();
        let ca = (alt + surface_radius) / earth_radius;
        let sa = c2 * crlat * srlat / (earth_radius * surface_radius);

        for m in 2..=self.maxord {
            sp[m] = sp[1].mul_add(cp[m - 1], cp[1] * sp[m - 1]);
            cp[m] = cp[1].mul_add(cp[m - 1], -(sp[1] * sp[m - 1]));
        }

        GeoMagFrame {
            ct,
            st,
            ca,
            sa,
            aor: re / earth_radius,
        }
    }

    fn finalize_result(bt: f64, br: f64, bp: f64, ca: f64, sa: f64) -> Result<GeoMagResult> {
        let bx = (-bt).mul_add(ca, -(br * sa));
        let by = bp;
        let bh = bx.hypot(by);
        let result = GeoMagResult {
            d: by.atan2(bx).to_degrees(),
            h: bh,
        };

        if result.h < BLACKOUT_ZONE {
            return Err(anyhow!("in blackout zone (H={:.1})", result.h));
        }
        if result.h < CAUTION_ZONE {
            // Keep behavior compatible with previous path where warning zone did not error out.
        }
        Ok(result)
    }

    fn calculate(
        &self,
        glat: f64,
        glon: f64,
        alt: f64,
        time: f64,
        scratch: &mut GeoMagScratch,
    ) -> Result<GeoMagResult> {
        let dt = time - self.epoch;
        if !(0.0..=5.0).contains(&dt) {
            return Err(anyhow!("time extends beyond model 5-year life span"));
        }

        scratch.prepare();
        let dp = &mut scratch.dp;
        let sp = &mut scratch.sp;
        let cp = &mut scratch.cp;
        let pp = &mut scratch.pp;
        let legendre = &mut scratch.p;
        let size = self.size;
        let coeff_stride = WMM_MAX_SIZE;
        let frame = self.initialize_frame(glat, glon, alt, sp, cp);
        let mut ar = frame.aor * frame.aor;
        let mut br = 0.0;
        let mut bt = 0.0;
        let mut bp = 0.0;
        let mut bpp = 0.0;

        for n in 1..=self.maxord {
            ar *= frame.aor;
            let fn_n = self.fn_values[n];
            for m in 0..=n {
                let coeff_m_base = m * coeff_stride;
                let coeff_mn = coeff_m_base + n;
                let p_m_base = m * size;
                let p_mn = p_m_base + n;
                let dp_mn;
                let p_mn_value;

                if n == m {
                    let prev_diag = (m - 1) * size + (n - 1);
                    let prev_diag_value = legendre[prev_diag];
                    let dp_prev_diag = dp[(m - 1) * coeff_stride + (n - 1)];
                    p_mn_value = frame.st * prev_diag_value;
                    dp_mn = frame.st.mul_add(dp_prev_diag, frame.ct * prev_diag_value);
                } else if n == 1 && m == 0 {
                    let prev_value = legendre[p_m_base + (n - 1)];
                    let dp_prev = dp[coeff_m_base + (n - 1)];
                    p_mn_value = frame.ct * prev_value;
                    dp_mn = frame.ct.mul_add(dp_prev, -(frame.st * prev_value));
                } else {
                    if m > n - 2 {
                        legendre[p_m_base + (n - 2)] = 0.0;
                        dp[coeff_m_base + (n - 2)] = 0.0;
                    }
                    let prev1 = p_m_base + (n - 1);
                    let prev2 = p_m_base + (n - 2);
                    let k_mn = self.k[coeff_mn];
                    let prev1_value = legendre[prev1];
                    p_mn_value = frame.ct.mul_add(prev1_value, -(k_mn * legendre[prev2]));
                    dp_mn = k_mn.mul_add(
                        -dp[coeff_m_base + (n - 2)],
                        frame
                            .ct
                            .mul_add(dp[coeff_m_base + (n - 1)], -(frame.st * prev1_value)),
                    );
                }
                legendre[p_mn] = p_mn_value;
                dp[coeff_mn] = dp_mn;

                let tc_mn = dt.mul_add(self.cd[coeff_mn], self.c[coeff_mn]);
                let cp_m = cp[m];
                let sp_m = sp[m];

                let par = ar * p_mn_value;
                let (temp1, temp2) = if m == 0 {
                    (tc_mn * cp_m, tc_mn * sp_m)
                } else {
                    let coeff_nm1 = n * coeff_stride + (m - 1);
                    let tc_nm1 = dt.mul_add(self.cd[coeff_nm1], self.c[coeff_nm1]);
                    (tc_mn.mul_add(cp_m, tc_nm1 * sp_m), tc_mn.mul_add(sp_m, -(tc_nm1 * cp_m)))
                };

                bt -= ar * temp1 * dp_mn;
                bp += self.fm_values[m] * temp2 * par;
                br += fn_n * temp1 * par;

                if frame.st == 0.0 && m == 1 {
                    if n == 1 {
                        pp[n] = pp[n - 1];
                    } else {
                        pp[n] = frame.ct.mul_add(pp[n - 1], -(self.k[coeff_mn] * pp[n - 2]));
                    }
                    let parp = ar * pp[n];
                    bpp += self.fm_values[m] * temp2 * parp;
                }
            }
        }

        if frame.st == 0.0 {
            bp = bpp;
        } else {
            bp /= frame.st;
        }

        Self::finalize_result(bt, br, bp, frame.ca, frame.sa)
    }
}

#[derive(Clone)]
struct GeoMagModel {
    inner: std::sync::Arc<GeoMagHolder>,
}

static HIGH_RESOLUTION_MODEL: OnceLock<GeoMagModel> = OnceLock::new();

struct GeoMagHolder {
    geo_mag: Box<GeoMag>,
}

unsafe impl Send for GeoMagHolder {}
unsafe impl Sync for GeoMagHolder {}

impl GeoMagModel {
    fn new(high_resolution: bool) -> Result<Self> {
        let coeffs = if high_resolution {
            WMM_HIGH_RESOLUTION
        } else {
            WMM_STANDARD
        };
        let geo_mag = Box::new(GeoMag::new(high_resolution, coeffs)?);
        Ok(Self {
            inner: std::sync::Arc::new(GeoMagHolder { geo_mag }),
        })
    }

    fn declination(
        &self,
        lat: f64,
        lon: f64,
        time: f64,
        scratch: &mut GeoMagScratch,
    ) -> Result<f64> {
        let result = self
            .inner
            .geo_mag
            .calculate(lat, lon, 0.0, time, scratch)
            .with_context(|| format!("geomag calculation failed for lat={lat}, lon={lon}"))?;
        Ok((result.d * 10.0).round() / 10.0)
    }
}

thread_local! {
    static THREAD_LOCAL_STATE: RefCell<ThreadLocalGeoMagState> = RefCell::new(ThreadLocalGeoMagState::new());
}

fn current_decimal_year() -> f64 {
    use chrono::{Datelike, Local};

    let now = Local::now();
    f64::from(now.year()) + ((f64::from(now.month()) - 1.0) / 12.0) + (f64::from(now.day()) / 365.0)
}

const fn cache_key(lat: f64, lon: f64) -> (u64, u64) {
    (lat.to_bits(), lon.to_bits())
}

fn shared_high_resolution_model() -> Result<GeoMagModel> {
    if let Some(model) = HIGH_RESOLUTION_MODEL.get() {
        return Ok(model.clone());
    }

    let model = GeoMagModel::new(true)?;
    let _ = HIGH_RESOLUTION_MODEL.set(model.clone());
    Ok(HIGH_RESOLUTION_MODEL.get().cloned().unwrap_or(model))
}

fn declination_for_current_thread(lat: f64, lon: f64, time: f64, use_cache: bool) -> Result<f64> {
    THREAD_LOCAL_STATE.with(|state_cell| {
        let mut state = state_cell.borrow_mut();
        let key = cache_key(lat, lon);

        if use_cache {
            if let Some(value) = state.cache.get(&key).copied() {
                return Ok(value);
            }
        }

        let model = shared_high_resolution_model()?;
        let value = model.declination(lat, lon, time, &mut state.scratch)?;

        if use_cache {
            state.cache.insert(key, value);
        }

        Ok(value)
    })
}

fn declination_batch_for_current_thread(
    coordinates: &[(f64, f64)],
    time: f64,
    use_cache: bool,
) -> Result<Vec<f64>> {
    THREAD_LOCAL_STATE.with(|state_cell| {
        let mut state = state_cell.borrow_mut();
        let model = shared_high_resolution_model()?;
        let mut values = Vec::with_capacity(coordinates.len());

        for &(lat, lon) in coordinates {
            let key = cache_key(lat, lon);
            if use_cache {
                if let Some(value) = state.cache.get(&key).copied() {
                    values.push(value);
                    continue;
                }
            }

            let value = model.declination(lat, lon, time, &mut state.scratch)?;
            if use_cache {
                state.cache.insert(key, value);
            }
            values.push(value);
        }

        Ok(values)
    })
}

#[cfg(test)]
fn compute_cached_declination(lat: f64, lon: f64, time: f64, use_cache: bool) -> Result<f64> {
    declination_for_current_thread(lat, lon, time, use_cache)
}

pub fn batch_get_magnetic_variations_internal(
    coordinates: &[(f64, f64)],
) -> Result<Vec<f64>> {
    let decimal_year = current_decimal_year();
    let mut unique_indices = HashMap::with_capacity(coordinates.len());
    let mut unique_coordinates = Vec::with_capacity(coordinates.len());
    let mut coordinate_indices = Vec::with_capacity(coordinates.len());

    for &(lat, lon) in coordinates {
        let key = cache_key(lat, lon);
        let index = match unique_indices.entry(key) {
            std::collections::hash_map::Entry::Occupied(entry) => *entry.get(),
            std::collections::hash_map::Entry::Vacant(entry) => {
                let index = unique_coordinates.len();
                unique_coordinates.push((lat, lon));
                entry.insert(index);
                index
            }
        };
        coordinate_indices.push(index);
    }

    let unique_values: Vec<f64> = if unique_coordinates.len() < MAGNETIC_PARALLEL_THRESHOLD {
        declination_batch_for_current_thread(&unique_coordinates, decimal_year, false)?
    } else {
        unique_coordinates
            .par_iter()
            .map(|&(lat, lon)| declination_for_current_thread(lat, lon, decimal_year, false))
            .collect::<Result<Vec<_>>>()?
    };

    let mut results = Vec::with_capacity(coordinate_indices.len());
    for index in coordinate_indices {
        results.push(unique_values[index]);
    }

    Ok(results)
}

#[cfg(test)]
fn get_magnetic_variation(lat: f64, lon: f64, use_cache: bool) -> Result<f64> {
    compute_cached_declination(lat, lon, current_decimal_year(), use_cache)
}

#[cfg(test)]
fn batch_get_magnetic_variations(coordinates: Vec<(f64, f64)>) -> Result<Vec<f64>> {
    batch_get_magnetic_variations_internal(&coordinates)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn computes_repeatable_declination() {
        let first = get_magnetic_variation(30.0, 120.0, true).unwrap();
        let second = get_magnetic_variation(30.0, 120.0, true).unwrap();
        assert!((first - second).abs() < 1e-9);
    }

    #[test]
    fn computes_batch_declinations() {
        let values = batch_get_magnetic_variations(vec![(30.0, 120.0), (31.0, 121.0)]).unwrap();
        assert_eq!(values.len(), 2);
    }

    #[test]
    fn reuses_scratch_without_state_leakage() {
        let first = compute_cached_declination(30.0, 120.0, current_decimal_year(), false).unwrap();
        let _different =
            compute_cached_declination(48.5, 87.2, current_decimal_year(), false).unwrap();
        let repeated =
            compute_cached_declination(30.0, 120.0, current_decimal_year(), false).unwrap();

        assert!((first - repeated).abs() < 1e-9);
    }
}
