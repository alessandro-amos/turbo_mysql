use mysql_async::{Conn, Pool};
use std::os::raw::{c_int, c_longlong, c_uchar};
use std::sync::Arc;
use tokio::sync::Mutex;

/// Represents a managed pool of MySQL connections.
pub struct MysqlPool {
    pub pool: Pool,
}

/// Represents a single, isolated MySQL connection.
pub struct MysqlConnection {
    pub conn: Arc<Mutex<Option<Conn>>>,
}

/// Represents a prepared statement bound to a specific connection.
pub struct MysqlPreparedStatement {
    pub conn: Arc<Mutex<Option<Conn>>>,
    pub stmt: mysql_async::Statement,
}

/// Function signature for the C callback used to send responses back to Dart.
pub type CallbackType = extern "C" fn(c_longlong, *mut c_uchar, c_int);

/// A thread-safe wrapper around the C callback function pointer.
pub struct CallbackWrapper(pub CallbackType);
unsafe impl Send for CallbackWrapper {}
unsafe impl Sync for CallbackWrapper {}
