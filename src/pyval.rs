//! Python-faithful value helpers.
//!
//! The python controller operated on plain JSON values, and its error
//! strings embed python's own rendering of those values. The parity bar
//! (fixtures byte-exact against the python outputs) means the rust side
//! must type-check and format values the way python did, quirks
//! included: `isinstance(True, int)` is true in python, dict iteration
//! is insertion-ordered, and json.dumps separates with ", " and ": ".
//!
//! Known edges where python and this module can diverge (named so the
//! fixture set can decide whether they are real): python str() of
//! extreme floats uses exponent form ("1e+30") where rust does not;
//! python json.dumps escapes non-ascii (ensure_ascii=True) where the
//! formatter here passes it through.

use serde_json::Value;
use serde_json::ser::{Formatter, WriteExt};
use std::io;

/// type(x).__name__ for JSON-decoded values, as python renders it.
pub fn py_type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "NoneType",
        Value::Bool(_) => "bool",
        Value::Number(n) => {
            if n.is_f64() {
                "float"
            } else {
                "int"
            }
        }
        Value::String(_) => "str",
        Value::Array(_) => "list",
        Value::Object(_) => "dict",
    }
}

/// isinstance(x, int) - python bools are ints.
pub fn py_is_int(v: &Value) -> bool {
    match v {
        Value::Bool(_) => true,
        Value::Number(n) => !n.is_f64(),
        _ => false,
    }
}

/// isinstance(x, (int, float)) - python bools count as numbers.
pub fn py_is_number(v: &Value) -> bool {
    matches!(v, Value::Bool(_) | Value::Number(_))
}

/// float(x) over a JSON number (python bools are 1/0). Only meaningful
/// where the caller already knows the value is numeric.
pub fn py_f64(v: &Value) -> f64 {
    match v {
        Value::Bool(b) => {
            if *b {
                1.0
            } else {
                0.0
            }
        }
        Value::Number(n) => n.as_f64().unwrap_or(0.0),
        _ => 0.0,
    }
}

/// str(x) for numbers as python renders them inside f-strings:
/// ints as digits, floats with their ".0" when integral.
pub fn py_num_str(v: &Value) -> String {
    match v {
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                i.to_string()
            } else if let Some(u) = n.as_u64() {
                u.to_string()
            } else if let Some(f) = n.as_f64() {
                let s = format!("{}", f);
                if s.contains('.') || s.contains('e') || s.contains("inf") || s.contains("NaN") {
                    s
                } else {
                    format!("{}.0", s)
                }
            } else {
                n.to_string()
            }
        }
        other => other.to_string(),
    }
}

/// python truthiness for JSON values: null, false, 0, "", [], {} are
/// falsy; everything else truthy.
pub fn py_truthy(v: &Value) -> bool {
    match v {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64().map(|f| f != 0.0).unwrap_or(true),
        Value::String(s) => !s.is_empty(),
        Value::Array(a) => !a.is_empty(),
        Value::Object(o) => !o.is_empty(),
    }
}

/// str(float) as python renders it - integral floats keep their ".0".
pub fn py_f64_str(f: f64) -> String {
    let s = format!("{}", f);
    if s.contains('.') || s.contains('e') || s.contains("inf") || s.contains("NaN") {
        s
    } else {
        format!("{}.0", s)
    }
}

/// json.dumps(v) with python's default formatting: separators ", " and
/// ": " on a single line. (ensure_ascii is NOT replicated - see the
/// module note.)
pub fn py_json_dumps(v: &Value) -> String {
    let mut out = Vec::new();
    let mut ser = serde_json::Serializer::with_formatter(&mut out, PyFormatter);
    serde::Serialize::serialize(v, &mut ser).expect("serialize cannot fail on Value");
    String::from_utf8(out).expect("formatter writes utf-8")
}

struct PyFormatter;

impl Formatter for PyFormatter {
    #[inline]
    fn begin_array<W: ?Sized + io::Write>(&mut self, writer: &mut W) -> io::Result<()> {
        writer.write_all(b"[")
    }

    #[inline]
    fn end_array<W: ?Sized + io::Write>(&mut self, writer: &mut W) -> io::Result<()> {
        writer.write_all(b"]")
    }

    #[inline]
    fn begin_array_value<W: ?Sized + io::Write>(
        &mut self,
        writer: &mut W,
        first: bool,
    ) -> io::Result<()> {
        if first {
            writer.write_all(b"")
        } else {
            writer.write_all(b", ")
        }
    }

    #[inline]
    fn begin_object<W: ?Sized + io::Write>(&mut self, writer: &mut W) -> io::Result<()> {
        writer.write_all(b"{")
    }

    #[inline]
    fn end_object<W: ?Sized + io::Write>(&mut self, writer: &mut W) -> io::Result<()> {
        writer.write_all(b"}")
    }

    #[inline]
    fn begin_object_key<W: ?Sized + io::Write>(
        &mut self,
        writer: &mut W,
        first: bool,
    ) -> io::Result<()> {
        if first {
            writer.write_all(b"")
        } else {
            writer.write_all(b", ")
        }
    }

    #[inline]
    fn begin_object_value<W: ?Sized + io::Write>(&mut self, writer: &mut W) -> io::Result<()> {
        writer.write_all(b": ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn python_type_names() {
        assert_eq!(py_type_name(&json!(null)), "NoneType");
        assert_eq!(py_type_name(&json!(true)), "bool");
        assert_eq!(py_type_name(&json!(1)), "int");
        assert_eq!(py_type_name(&json!(1.5)), "float");
        assert_eq!(py_type_name(&json!(2.0)), "float");
        assert_eq!(py_type_name(&json!("x")), "str");
        assert_eq!(py_type_name(&json!([])), "list");
        assert_eq!(py_type_name(&json!({})), "dict");
    }

    #[test]
    fn python_bools_are_numbers() {
        assert!(py_is_int(&json!(true)));
        assert!(py_is_number(&json!(true)));
        assert!(!py_is_int(&json!(1.0)));
        assert!(py_is_int(&json!(1)));
        assert_eq!(py_f64(&json!(true)), 1.0);
    }

    #[test]
    fn python_number_rendering() {
        assert_eq!(py_num_str(&json!(4096)), "4096");
        assert_eq!(py_num_str(&json!(2.0)), "2.0");
        assert_eq!(py_num_str(&json!(1.5)), "1.5");
    }

    #[test]
    fn python_json_dumps_formatting() {
        let v = json!({"a": 1, "b": [1, 2], "c": "x"});
        assert_eq!(py_json_dumps(&v), r#"{"a": 1, "b": [1, 2], "c": "x"}"#);
        assert_eq!(py_json_dumps(&json!({})), "{}");
        assert_eq!(py_json_dumps(&json!([])), "[]");
    }
}
