use crate::types::CallbackWrapper;
use mysql_async::{Row, Value as MySqlValue};
use std::ffi::CStr;
use std::os::raw::{c_char, c_int, c_longlong, c_uchar};
use std::slice;

const STATUS_ERROR: u8 = 0;
const STATUS_OK: u8 = 1;

const PARAM_NULL: u8 = 0;
const PARAM_INT: u8 = 1;
const PARAM_FLOAT: u8 = 2;
const PARAM_STRING: u8 = 3;
const PARAM_BLOB: u8 = 4;

macro_rules! unwrap_or_return {
    ($expr:expr, $cb:expr, $id:expr) => {
        match $expr {
            Ok(val) => val,
            Err(e) => {
                crate::utils::send_error(&$cb, $id, &e.to_string());
                return;
            }
        }
    };
    ($expr:expr, $cb:expr, $id:expr, $msg:expr) => {
        match $expr {
            Some(val) => val,
            None => {
                crate::utils::send_error(&$cb, $id, $msg);
                return;
            }
        }
    };
}

pub trait BinaryWrite {
    fn write_u8(&mut self, v: u8);
    fn write_u16(&mut self, v: u16);
    fn write_u32(&mut self, v: u32);
    fn write_u64(&mut self, v: u64);
    fn write_i64(&mut self, v: i64);
    fn write_f64(&mut self, v: f64);
    fn write_blob(&mut self, v: &[u8]);
}

impl BinaryWrite for Vec<u8> {
    fn write_u8(&mut self, v: u8) {
        self.push(v);
    }
    fn write_u16(&mut self, v: u16) {
        self.extend_from_slice(&v.to_le_bytes());
    }
    fn write_u32(&mut self, v: u32) {
        self.extend_from_slice(&v.to_le_bytes());
    }
    fn write_u64(&mut self, v: u64) {
        self.extend_from_slice(&v.to_le_bytes());
    }
    fn write_i64(&mut self, v: i64) {
        self.extend_from_slice(&v.to_le_bytes());
    }
    fn write_f64(&mut self, v: f64) {
        self.extend_from_slice(&v.to_le_bytes());
    }
    fn write_blob(&mut self, v: &[u8]) {
        self.write_u32(v.len() as u32);
        self.extend_from_slice(v);
    }
}

pub struct BinaryReader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> BinaryReader<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }
    pub fn read_u8(&mut self) -> Option<u8> {
        if self.pos < self.data.len() {
            let v = self.data[self.pos];
            self.pos += 1;
            Some(v)
        } else {
            None
        }
    }
    pub fn read_i64(&mut self) -> Option<i64> {
        if self.pos + 8 <= self.data.len() {
            let bytes = self.data[self.pos..self.pos + 8].try_into().ok()?;
            self.pos += 8;
            Some(i64::from_le_bytes(bytes))
        } else {
            None
        }
    }
    pub fn read_f64(&mut self) -> Option<f64> {
        if self.pos + 8 <= self.data.len() {
            let bytes = self.data[self.pos..self.pos + 8].try_into().ok()?;
            self.pos += 8;
            Some(f64::from_le_bytes(bytes))
        } else {
            None
        }
    }
    pub fn read_u32(&mut self) -> Option<u32> {
        if self.pos + 4 <= self.data.len() {
            let bytes = self.data[self.pos..self.pos + 4].try_into().ok()?;
            self.pos += 4;
            Some(u32::from_le_bytes(bytes))
        } else {
            None
        }
    }
    pub fn read_blob(&mut self) -> Option<Vec<u8>> {
        let len = self.read_u32()? as usize;
        if self.pos + len <= self.data.len() {
            let bytes = self.data[self.pos..self.pos + len].to_vec();
            self.pos += len;
            Some(bytes)
        } else {
            None
        }
    }
}

pub fn send_response(cb: &CallbackWrapper, req_id: c_longlong, data: Vec<u8>) {
    let mut buf = data.into_boxed_slice();
    let len = buf.len() as c_int;
    let ptr = buf.as_mut_ptr();
    std::mem::forget(buf);
    (cb.0)(req_id, ptr, len);
}

pub fn send_error(cb: &CallbackWrapper, req_id: c_longlong, msg: &str) {
    send_response(cb, req_id, encode_error(msg));
}

pub fn encode_error(msg: &str) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.write_u8(STATUS_ERROR);
    buf.write_blob(msg.as_bytes());
    buf
}

pub fn parse_value(reader: &mut BinaryReader) -> MySqlValue {
    match reader.read_u8() {
        Some(PARAM_NULL) => MySqlValue::NULL,
        Some(PARAM_INT) => reader
            .read_i64()
            .map(MySqlValue::Int)
            .unwrap_or(MySqlValue::NULL),
        Some(PARAM_FLOAT) => reader
            .read_f64()
            .map(MySqlValue::Double)
            .unwrap_or(MySqlValue::NULL),
        Some(PARAM_STRING) | Some(PARAM_BLOB) => reader
            .read_blob()
            .map(MySqlValue::Bytes)
            .unwrap_or(MySqlValue::NULL),
        _ => MySqlValue::NULL,
    }
}

pub fn parse_params_list(ptr: *const c_uchar, len: c_int) -> Vec<MySqlValue> {
    if ptr.is_null() || len <= 0 {
        return Vec::new();
    }
    let data = unsafe { slice::from_raw_parts(ptr, len as usize) };
    let mut reader = BinaryReader::new(data);
    let count = reader.read_u32().unwrap_or(0);
    let mut mysql_params = Vec::with_capacity(count as usize);
    for _ in 0..count {
        mysql_params.push(parse_value(&mut reader));
    }
    mysql_params
}

pub fn serialize_result(rows: Vec<Row>, affected_rows: u64, last_insert_id: u64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(20 + rows.len() * 64);
    buf.write_u8(STATUS_OK);
    buf.write_u64(affected_rows);
    buf.write_u64(last_insert_id);

    if rows.is_empty() {
        buf.write_u32(0);
        buf.write_u32(0);
        return buf;
    }

    let cols_meta: Vec<(Vec<u8>, u16, u16)> = {
        let cols = rows[0].columns_ref();
        cols.iter()
            .map(|c| {
                (
                    c.name_str().as_bytes().to_vec(),
                    c.column_type() as u16,
                    c.character_set(),
                )
            })
            .collect()
    };

    let cols_len = cols_meta.len();
    buf.write_u32(cols_len as u32);

    for (name, col_type, charset) in &cols_meta {
        buf.write_blob(name);
        buf.write_u16(*col_type);
        buf.write_u16(*charset);
    }

    buf.write_u32(rows.len() as u32);

    for row in rows {
        for i in 0..cols_len {
            let val = if i < row.len() { &row[i] } else { &MySqlValue::NULL };
            match val {
                MySqlValue::NULL => buf.write_u8(0),
                MySqlValue::Int(v) => {
                    buf.write_u8(1);
                    buf.write_blob(&v.to_le_bytes());
                }
                MySqlValue::UInt(v) => {
                    buf.write_u8(1);
                    buf.write_blob(&v.to_le_bytes());
                }
                MySqlValue::Float(v) => {
                    buf.write_u8(1);
                    buf.write_blob(&(*v as f64).to_le_bytes());
                }
                MySqlValue::Double(v) => {
                    buf.write_u8(1);
                    buf.write_blob(&v.to_le_bytes());
                }
                MySqlValue::Bytes(b) => {
                    buf.write_u8(1);
                    buf.write_blob(b);
                }
                MySqlValue::Date(y, mo, d, h, min, s, mic) => {
                    let ds = format!(
                        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}",
                        y, mo, d, h, min, s, mic
                    );
                    buf.write_u8(1);
                    buf.write_blob(ds.as_bytes());
                }
                MySqlValue::Time(neg, d, h, m, s, mic) => {
                    let sign = if *neg { "-" } else { "" };
                    let total_hours = (*d) * 24 + (*h as u32);
                    let ts = format!("{}{:02}:{:02}:{:02}.{:06}", sign, total_hours, m, s, mic);
                    buf.write_u8(1);
                    buf.write_blob(ts.as_bytes());
                }
            }
        }
    }

    buf
}

pub fn ptr_to_string(ptr: *const c_char) -> Result<String, String> {
    if ptr.is_null() {
        return Err("Null pointer".to_string());
    }
    unsafe { CStr::from_ptr(ptr) }
        .to_str()
        .map(|s| s.to_string())
        .map_err(|_| "Invalid UTF-8".to_string())
}

pub fn ptr_to_vec(ptr: *const c_uchar, len: c_int) -> Vec<u8> {
    if ptr.is_null() || len <= 0 {
        Vec::new()
    } else {
        unsafe { slice::from_raw_parts(ptr, len as usize).to_vec() }
    }
}