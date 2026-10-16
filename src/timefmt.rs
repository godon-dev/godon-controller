//! Timestamps - the classic silent port fault, handled where everyone
//! can see it. Three shapes the estate's clients read today:
//!
//! - naive now() strings: "T" separator, microseconds only when
//!   nonzero (python datetime.now().isoformat())
//! - aware UTC now() strings: the same, with a "+00:00" offset
//!   (python datetime.now(timezone.utc).isoformat())
//! - database timestamptz cells rendered as aware isoformat:
//!   "+HH:MM" offset with colon, six-digit fraction only when nonzero
//!   (what psycopg2's datetimes produced)

use chrono::{Local, Timelike, Utc};

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

/// python's datetime.now(timezone.utc).isoformat(): "T" separator,
/// "+00:00" offset, microseconds only when nonzero. The relaunch
/// history's ts is the estate's live reader of this shape.
pub fn now_iso_utc_aware() -> String {
    let dt = Utc::now().naive_utc();
    let base = dt.format("%Y-%m-%dT%H:%M:%S").to_string();
    let micros = (dt.nanosecond() / 1000) % 1_000_000;
    if micros != 0 {
        format!("{}.{:06}+00:00", base, micros)
    } else {
        format!("{}+00:00", base)
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
        let scale = 10u32.pow(6 - frac_digits.len() as u32);
        let micros: u32 = frac_digits.parse::<u32>().unwrap_or(0) * scale;
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
