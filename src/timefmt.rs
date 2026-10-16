//! Timestamps - the classic silent port fault, handled where everyone
//! can see it. Two shapes the estate's clients read today:
//!
//! - naive now() strings: "T" separator, microseconds only when
//!   nonzero (python datetime.now().isoformat())
//! - database timestamptz cells rendered as aware isoformat:
//!   "+HH:MM" offset with colon, six-digit fraction only when nonzero
//!   (what psycopg2's datetimes produced)

use chrono::Local;

pub fn now_iso_naive() -> String {
    let dt = Local::now().naive_local();
    let base = dt.format("%Y-%m-%dT%H:%M:%S").to_string();
    let micros = (dt.nanosecond() / 1000) % 1_000_000;
    if micros != 0 {
        format!("{}.{:06}", base, micros)
    } else {
        base
    }
}

/// Render a postgres timestamptz text cell ("2026-10-15 18:12:52.123456+00")
/// the way python's aware isoformat rendered it.
pub fn pg_ts_to_iso(cell: &str) -> Option<String> {
    let s = cell.trim();
    // split the offset off the end (postgres prints +HH or +HH:MM; may omit)
    let (stem, offset) = match s.rfind(['+', '-']) {
        Some(pos) if pos > 10 => (&s[..pos], &s[pos..]),
        _ => (s, "+00:00"),
    };
    let offset_full = if offset.len() == 3 {
        format!("{}:00", offset)
    } else {
        offset.to_string()
    };
    // stem: date space time[.fraction]
    let (dt, frac) = match stem.find('.') {
        Some(p) => (&stem[..p], &stem[p + 1..]),
        None => (stem, ""),
    };
    let mut base = dt.replace(' ', "T");
    let frac_digits: String = frac.chars().take(6).collect();
    if !frac_digits.is_empty() {
        let micros: u32 = frac_digits.parse().ok()?;
        if micros != 0 {
            base = format!("{}.{:06}", base, micros);
        }
    }
    Some(format!("{}{}", base, offset_full))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pg_ts_rendering_matches_python_isoformat() {
        assert_eq!(
            pg_ts_to_iso("2026-10-15 18:12:52.123456+00").unwrap(),
            "2026-10-15T18:12:52.123456+00:00"
        );
        assert_eq!(
            pg_ts_to_iso("2026-10-15 18:12:52+00").unwrap(),
            "2026-10-15T18:12:52+00:00"
        );
        assert_eq!(
            pg_ts_to_iso("2026-10-15 18:12:52.5+02").unwrap(),
            "2026-10-15T18:12:52.500000+02:00"
        );
        assert_eq!(
            pg_ts_to_iso("2026-10-15 18:12:52.000000+00").unwrap(),
            "2026-10-15T18:12:52+00:00"
        );
    }
}
