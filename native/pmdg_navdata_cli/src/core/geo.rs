pub const EARTH_RADIUS_KM: f64 = 6371.0;
pub const KM_TO_NM: f64 = 0.539_957;
pub const EARTH_RADIUS_M: f64 = 6_371_000.0;

pub fn haversine_km(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    let lon1r = lon1.to_radians();
    let lat1r = lat1.to_radians();
    let lon2r = lon2.to_radians();
    let lat2r = lat2.to_radians();

    let dlon = lon2r - lon1r;
    let dlat = lat2r - lat1r;

    let a = (dlat / 2.0).sin().mul_add((dlat / 2.0).sin(), lat1r.cos() * lat2r.cos() * (dlon / 2.0).sin().powi(2));
    let c = 2.0 * a.sqrt().asin();
    EARTH_RADIUS_KM * c
}

pub fn magnetic_bearing(
    lat1: f64,
    lon1: f64,
    lat2: f64,
    lon2: f64,
    declination: f64,
) -> f64 {
    let lat1r = lat1.to_radians();
    let lon1r = lon1.to_radians();
    let lat2r = lat2.to_radians();
    let lon2r = lon2.to_radians();

    let dlon = lon2r - lon1r;
    let x = dlon.sin() * lat2r.cos();
    let y = lat1r.cos().mul_add(lat2r.sin(), -(lat1r.sin() * lat2r.cos() * dlon.cos()));
    let true_bearing = (x.atan2(y).to_degrees() + 360.0) % 360.0;
    (true_bearing - declination + 360.0) % 360.0
}

pub fn haversine_m(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    let lat1r = lat1.to_radians();
    let lon1r = lon1.to_radians();
    let lat2r = lat2.to_radians();
    let lon2r = lon2.to_radians();

    let dlat = lat2r - lat1r;
    let dlon = lon2r - lon1r;
    let a = (dlat / 2.0).sin().mul_add((dlat / 2.0).sin(), lat1r.cos() * lat2r.cos() * (dlon / 2.0).sin().powi(2));
    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());
    EARTH_RADIUS_M * c
}
