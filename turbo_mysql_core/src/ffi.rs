use crate::get_runtime;
use crate::types::{
    CallbackType, CallbackWrapper, MysqlConnection, MysqlPool, MysqlPreparedStatement,
};
use crate::utils::{
    BinaryWrite, parse_params_list, ptr_to_string, ptr_to_vec, send_error, send_response,
    serialize_result,
};
use mysql_async::prelude::*;
use mysql_async::{Opts, Params, Pool};
use std::ffi::CStr;
use std::os::raw::{c_char, c_int, c_longlong, c_uchar};
use std::sync::Arc;
use tokio::sync::Mutex;

macro_rules! parse_params {
    ($params_owned:expr) => {{
        let params_parsed = parse_params_list($params_owned.as_ptr(), $params_owned.len() as c_int);
        if params_parsed.is_empty() {
            Params::Empty
        } else {
            Params::Positional(params_parsed)
        }
    }};
}

macro_rules! execute_batch {
    ($conn:expr, $table_str:expr, $columns_str:expr, $data:expr, $req_id:expr, $cb:expr, $on_duplicate:expr) => {
        let mut reader = crate::utils::BinaryReader::new(&$data);
        let num_rows =
            unwrap_or_return!(reader.read_u32(), $cb, $req_id, "Failed to read row count") as usize;
        if num_rows == 0 {
            send_response(&$cb, $req_id, serialize_result(Vec::new(), 0, 0));
            return;
        }
        let column_names: Vec<&str> = $columns_str.split(',').collect();
        let num_cols = column_names.len();
        if num_cols == 0 {
            send_error(&$cb, $req_id, "No columns specified");
            return;
        }
        let total_values = num_rows * num_cols;
        let mut all_values = Vec::with_capacity(total_values);
        for _ in 0..total_values {
            all_values.push(crate::utils::parse_value(&mut reader));
        }

        let base_placeholders = vec!["?"; num_cols].join(",");
        let update_clause = if $on_duplicate {
            let updates: Vec<String> = column_names
                .iter()
                .map(|c| format!("{} = VALUES({})", c, c))
                .collect();
            format!(" ON DUPLICATE KEY UPDATE {}", updates.join(", "))
        } else {
            String::new()
        };

        let rows_per_chunk = (60000 / num_cols).max(1);
        let chunks = all_values.chunks(rows_per_chunk * num_cols);
        let mut total_affected = 0;
        let mut last_id = 0;

        for chunk in chunks {
            let params = Params::Positional(chunk.to_vec());
            let current_chunk_size = chunk.len() / num_cols;
            let chunk_placeholders: Vec<String> =
                std::iter::repeat(format!("({})", base_placeholders))
                    .take(current_chunk_size)
                    .collect();
            let chunk_query = format!(
                "INSERT INTO {} ({}) VALUES {}{}",
                $table_str,
                $columns_str,
                chunk_placeholders.join(","),
                update_clause
            );
            match $conn.exec_drop(chunk_query, params).await {
                Ok(_) => {
                    total_affected += $conn.affected_rows();
                    let current_id = $conn.last_insert_id().unwrap_or(0);
                    if current_id > 0 {
                        last_id = current_id;
                    }
                }
                Err(e) => {
                    send_error(&$cb, $req_id, &format!("Batch insert error: {}", e));
                    return;
                }
            }
        }
        send_response(
            &$cb,
            $req_id,
            serialize_result(Vec::new(), total_affected, last_id),
        );
    };
}

#[unsafe(no_mangle)]
pub extern "C" fn mysql_pool_create(url: *const c_char) -> *mut MysqlPool {
    if url.is_null() {
        return std::ptr::null_mut();
    }
    let url_str = match unsafe { CStr::from_ptr(url) }.to_str() {
        Ok(s) => s,
        Err(..) => return std::ptr::null_mut(),
    };
    let opts = match Opts::from_url(url_str) {
        Ok(opts) => opts,
        Err(..) => return std::ptr::null_mut(),
    };
    Box::into_raw(Box::new(MysqlPool {
        pool: Pool::new(opts),
    }))
}

#[unsafe(no_mangle)]
pub extern "C" fn mysql_pool_destroy(pool_ptr: *mut MysqlPool) {
    if !pool_ptr.is_null() {
        unsafe {
            let _ = Box::from_raw(pool_ptr);
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn mysql_pool_query_raw(
    pool_ptr: *mut MysqlPool,
    query: *const c_char,
    req_id: c_longlong,
    callback: CallbackType,
) {
    let cb = CallbackWrapper(callback);
    if pool_ptr.is_null() {
        send_error(&cb, req_id, "Invalid pointers");
        return;
    }
    let query_str = unwrap_or_return!(ptr_to_string(query), cb, req_id);
    let pool = unsafe { &*pool_ptr }.pool.clone();
    get_runtime().spawn(async move {
        let mut conn = unwrap_or_return!(pool.get_conn().await, cb, req_id);
        let rows = unwrap_or_return!(conn.query(query_str).await, cb, req_id);
        send_response(
            &cb,
            req_id,
            serialize_result(
                rows,
                conn.affected_rows(),
                conn.last_insert_id().unwrap_or(0),
            ),
        );
    });
}

#[unsafe(no_mangle)]
pub extern "C" fn mysql_pool_query(
    pool_ptr: *mut MysqlPool,
    query: *const c_char,
    params_ptr: *const c_uchar,
    params_len: c_int,
    req_id: c_longlong,
    callback: CallbackType,
) {
    let cb = CallbackWrapper(callback);
    if pool_ptr.is_null() {
        send_error(&cb, req_id, "Invalid pointers");
        return;
    }
    let query_str = unwrap_or_return!(ptr_to_string(query), cb, req_id);
    let params_owned = ptr_to_vec(params_ptr, params_len);
    let pool = unsafe { &*pool_ptr }.pool.clone();
    get_runtime().spawn(async move {
        let params_pos = parse_params!(params_owned);
        let mut conn = unwrap_or_return!(pool.get_conn().await, cb, req_id);
        let rows = unwrap_or_return!(conn.exec(query_str, params_pos).await, cb, req_id);
        send_response(
            &cb,
            req_id,
            serialize_result(
                rows,
                conn.affected_rows(),
                conn.last_insert_id().unwrap_or(0),
            ),
        );
    });
}

#[unsafe(no_mangle)]
pub extern "C" fn mysql_pool_prepare(
    pool_ptr: *mut MysqlPool,
    query: *const c_char,
    req_id: c_longlong,
    callback: CallbackType,
) {
    let cb = CallbackWrapper(callback);
    if pool_ptr.is_null() {
        send_error(&cb, req_id, "Invalid pointers");
        return;
    }
    let query_str = unwrap_or_return!(ptr_to_string(query), cb, req_id);
    let pool = unsafe { &*pool_ptr }.pool.clone();
    get_runtime().spawn(async move {
        let mut conn = unwrap_or_return!(pool.get_conn().await, cb, req_id);
        let stmt = unwrap_or_return!(conn.prep(query_str).await, cb, req_id);
        let ptr = Box::into_raw(Box::new(MysqlPreparedStatement {
            conn: Arc::new(Mutex::new(Some(conn))),
            stmt,
        }));
        let mut buf = Vec::new();
        buf.write_u8(1);
        buf.write_u64(ptr as u64);
        buf.write_u64(0);
        buf.write_u32(0);
        buf.write_u32(0);
        send_response(&cb, req_id, buf);
    });
}

#[unsafe(no_mangle)]
pub extern "C" fn mysql_pool_begin_transaction(
    pool_ptr: *mut MysqlPool,
    req_id: c_longlong,
    callback: CallbackType,
) {
    let cb = CallbackWrapper(callback);
    if pool_ptr.is_null() {
        send_error(&cb, req_id, "Invalid pointers");
        return;
    }
    let pool = unsafe { &*pool_ptr }.pool.clone();
    get_runtime().spawn(async move {
        let mut conn = unwrap_or_return!(pool.get_conn().await, cb, req_id);
        unwrap_or_return!(conn.query_drop("START TRANSACTION").await, cb, req_id);

        let ptr = Box::into_raw(Box::new(MysqlConnection {
            conn: Arc::new(Mutex::new(Some(conn))),
        }));

        let mut buf = Vec::new();
        buf.write_u8(1);
        buf.write_u64(ptr as u64);
        buf.write_u64(0);
        buf.write_u32(0);
        buf.write_u32(0);
        send_response(&cb, req_id, buf);
    });
}

#[unsafe(no_mangle)]
pub extern "C" fn mysql_conn_query_raw(
    conn_ptr: *mut MysqlConnection,
    query: *const c_char,
    req_id: c_longlong,
    callback: CallbackType,
) {
    let cb = CallbackWrapper(callback);
    if conn_ptr.is_null() {
        send_error(&cb, req_id, "Invalid connection pointer");
        return;
    }
    let query_str = unwrap_or_return!(ptr_to_string(query), cb, req_id);
    let conn_arc = unsafe { &*conn_ptr }.conn.clone();

    get_runtime().spawn(async move {
        let mut lock = conn_arc.lock().await;
        if let Some(conn) = lock.as_mut() {
            let rows = unwrap_or_return!(conn.query(query_str).await, cb, req_id);
            send_response(
                &cb,
                req_id,
                serialize_result(
                    rows,
                    conn.affected_rows(),
                    conn.last_insert_id().unwrap_or(0),
                ),
            );
        } else {
            send_error(&cb, req_id, "Connection is closed");
        }
    });
}

#[unsafe(no_mangle)]
pub extern "C" fn mysql_conn_query(
    conn_ptr: *mut MysqlConnection,
    query: *const c_char,
    params_ptr: *const c_uchar,
    params_len: c_int,
    req_id: c_longlong,
    callback: CallbackType,
) {
    let cb = CallbackWrapper(callback);
    if conn_ptr.is_null() {
        send_error(&cb, req_id, "Invalid connection pointer");
        return;
    }
    let query_str = unwrap_or_return!(ptr_to_string(query), cb, req_id);
    let params_owned = ptr_to_vec(params_ptr, params_len);
    let conn_arc = unsafe { &*conn_ptr }.conn.clone();

    get_runtime().spawn(async move {
        let params_pos = parse_params!(params_owned);
        let mut lock = conn_arc.lock().await;
        if let Some(conn) = lock.as_mut() {
            let rows = unwrap_or_return!(conn.exec(query_str, params_pos).await, cb, req_id);
            send_response(
                &cb,
                req_id,
                serialize_result(
                    rows,
                    conn.affected_rows(),
                    conn.last_insert_id().unwrap_or(0),
                ),
            );
        } else {
            send_error(&cb, req_id, "Connection is closed");
        }
    });
}

#[unsafe(no_mangle)]
pub extern "C" fn mysql_conn_commit(
    conn_ptr: *mut MysqlConnection,
    req_id: c_longlong,
    callback: CallbackType,
) {
    let cb = CallbackWrapper(callback);
    if conn_ptr.is_null() {
        send_error(&cb, req_id, "Invalid connection pointer");
        return;
    }
    let conn_arc = unsafe { &*conn_ptr }.conn.clone();
    get_runtime().spawn(async move {
        let mut lock = conn_arc.lock().await;
        if let Some(conn) = lock.as_mut() {
            unwrap_or_return!(conn.query_drop("COMMIT").await, cb, req_id);
            send_response(&cb, req_id, serialize_result(Vec::new(), 0, 0));
        } else {
            send_error(&cb, req_id, "Connection is closed");
        }
    });
}

#[unsafe(no_mangle)]
pub extern "C" fn mysql_conn_rollback(
    conn_ptr: *mut MysqlConnection,
    req_id: c_longlong,
    callback: CallbackType,
) {
    let cb = CallbackWrapper(callback);
    if conn_ptr.is_null() {
        send_error(&cb, req_id, "Invalid connection pointer");
        return;
    }
    let conn_arc = unsafe { &*conn_ptr }.conn.clone();
    get_runtime().spawn(async move {
        let mut lock = conn_arc.lock().await;
        if let Some(conn) = lock.as_mut() {
            unwrap_or_return!(conn.query_drop("ROLLBACK").await, cb, req_id);
            send_response(&cb, req_id, serialize_result(Vec::new(), 0, 0));
        } else {
            send_error(&cb, req_id, "Connection is closed");
        }
    });
}

pub(crate) async fn internal_conn_batch_execute(
    conn_arc: Arc<Mutex<Option<mysql_async::Conn>>>,
    table_str: String,
    columns_str: String,
    data: Vec<u8>,
    req_id: c_longlong,
    cb: CallbackWrapper,
    on_duplicate: bool,
) {
    let mut lock = conn_arc.lock().await;
    if let Some(conn) = lock.as_mut() {
        execute_batch!(conn, table_str, columns_str, data, req_id, cb, on_duplicate);
    } else {
        send_error(&cb, req_id, "Connection is closed");
    }
}

pub(crate) async fn internal_pool_batch_execute(
    pool: Pool,
    table_str: String,
    columns_str: String,
    data: Vec<u8>,
    req_id: c_longlong,
    cb: CallbackWrapper,
    on_duplicate: bool,
) {
    let mut conn = unwrap_or_return!(pool.get_conn().await, cb, req_id);
    execute_batch!(conn, table_str, columns_str, data, req_id, cb, on_duplicate);
}

#[unsafe(no_mangle)]
pub extern "C" fn mysql_conn_batch_insert(
    conn_ptr: *mut MysqlConnection,
    table: *const c_char,
    columns: *const c_char,
    data_ptr: *const c_uchar,
    data_len: c_int,
    req_id: c_longlong,
    callback: CallbackType,
) {
    let cb = CallbackWrapper(callback);
    if conn_ptr.is_null() {
        send_error(&cb, req_id, "Invalid connection pointer");
        return;
    }
    let table_str = unwrap_or_return!(ptr_to_string(table), cb, req_id);
    let columns_str = unwrap_or_return!(ptr_to_string(columns), cb, req_id);
    let data = ptr_to_vec(data_ptr, data_len);
    let conn_arc = unsafe { &*conn_ptr }.conn.clone();
    get_runtime().spawn(async move {
        internal_conn_batch_execute(conn_arc, table_str, columns_str, data, req_id, cb, false)
            .await;
    });
}

#[unsafe(no_mangle)]
pub extern "C" fn mysql_conn_batch_upsert(
    conn_ptr: *mut MysqlConnection,
    table: *const c_char,
    columns: *const c_char,
    data_ptr: *const c_uchar,
    data_len: c_int,
    req_id: c_longlong,
    callback: CallbackType,
) {
    let cb = CallbackWrapper(callback);
    if conn_ptr.is_null() {
        send_error(&cb, req_id, "Invalid connection pointer");
        return;
    }
    let table_str = unwrap_or_return!(ptr_to_string(table), cb, req_id);
    let columns_str = unwrap_or_return!(ptr_to_string(columns), cb, req_id);
    let data = ptr_to_vec(data_ptr, data_len);
    let conn_arc = unsafe { &*conn_ptr }.conn.clone();
    get_runtime().spawn(async move {
        internal_conn_batch_execute(conn_arc, table_str, columns_str, data, req_id, cb, true).await;
    });
}

#[unsafe(no_mangle)]
pub extern "C" fn mysql_pool_batch_insert(
    pool_ptr: *mut MysqlPool,
    table: *const c_char,
    columns: *const c_char,
    data_ptr: *const c_uchar,
    data_len: c_int,
    req_id: c_longlong,
    callback: CallbackType,
) {
    let cb = CallbackWrapper(callback);
    if pool_ptr.is_null() {
        send_error(&cb, req_id, "Invalid pointers");
        return;
    }
    let table_str = unwrap_or_return!(ptr_to_string(table), cb, req_id);
    let columns_str = unwrap_or_return!(ptr_to_string(columns), cb, req_id);
    let data = ptr_to_vec(data_ptr, data_len);
    let pool = unsafe { &*pool_ptr }.pool.clone();
    get_runtime().spawn(async move {
        internal_pool_batch_execute(pool, table_str, columns_str, data, req_id, cb, false).await;
    });
}

#[unsafe(no_mangle)]
pub extern "C" fn mysql_pool_batch_upsert(
    pool_ptr: *mut MysqlPool,
    table: *const c_char,
    columns: *const c_char,
    data_ptr: *const c_uchar,
    data_len: c_int,
    req_id: c_longlong,
    callback: CallbackType,
) {
    let cb = CallbackWrapper(callback);
    if pool_ptr.is_null() {
        send_error(&cb, req_id, "Invalid pointers");
        return;
    }
    let table_str = unwrap_or_return!(ptr_to_string(table), cb, req_id);
    let columns_str = unwrap_or_return!(ptr_to_string(columns), cb, req_id);
    let data = ptr_to_vec(data_ptr, data_len);
    let pool = unsafe { &*pool_ptr }.pool.clone();
    get_runtime().spawn(async move {
        internal_pool_batch_execute(pool, table_str, columns_str, data, req_id, cb, true).await;
    });
}

#[unsafe(no_mangle)]
pub extern "C" fn mysql_stmt_execute(
    stmt_ptr: *mut MysqlPreparedStatement,
    params_ptr: *const c_uchar,
    params_len: c_int,
    req_id: c_longlong,
    callback: CallbackType,
) {
    let cb = CallbackWrapper(callback);
    if stmt_ptr.is_null() {
        send_error(&cb, req_id, "Invalid statement pointer");
        return;
    }
    let stmt_ref = unsafe { &*stmt_ptr };
    let conn_arc = stmt_ref.conn.clone();
    let stmt = stmt_ref.stmt.clone();
    let params_owned = ptr_to_vec(params_ptr, params_len);
    get_runtime().spawn(async move {
        let params_pos = parse_params!(params_owned);
        let mut lock = conn_arc.lock().await;
        if let Some(conn) = lock.as_mut() {
            let rows = unwrap_or_return!(conn.exec(stmt, params_pos).await, cb, req_id);
            send_response(
                &cb,
                req_id,
                serialize_result(
                    rows,
                    conn.affected_rows(),
                    conn.last_insert_id().unwrap_or(0),
                ),
            );
        } else {
            send_error(&cb, req_id, "Connection is closed");
        }
    });
}

#[unsafe(no_mangle)]
pub extern "C" fn mysql_stmt_destroy(stmt_ptr: *mut MysqlPreparedStatement) {
    if !stmt_ptr.is_null() {
        unsafe {
            let _ = Box::from_raw(stmt_ptr);
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn mysql_conn_destroy(conn_ptr: *mut MysqlConnection) {
    if !conn_ptr.is_null() {
        unsafe {
            let _ = Box::from_raw(conn_ptr);
        }
    }
}
