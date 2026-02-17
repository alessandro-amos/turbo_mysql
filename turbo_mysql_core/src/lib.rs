pub mod types;
#[macro_use]
pub mod utils;
pub mod ffi;

use mimalloc::MiMalloc;
use std::os::raw::{c_int, c_uchar};
use std::sync::OnceLock;
use tokio::runtime::Runtime;

/// Global allocator using mimalloc for optimized memory management.
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

/// Global storage for the Tokio asynchronous runtime.
pub static RUNTIME: OnceLock<Runtime> = OnceLock::new();

/// Retrieves the global Tokio runtime, initializing it if necessary.
pub fn get_runtime() -> &'static Runtime {
    RUNTIME.get_or_init(|| Runtime::new().expect("Failed to create Tokio runtime"))
}

/// Frees a memory buffer allocated by the Rust FFI layer.
#[unsafe(no_mangle)]
pub extern "C" fn mysql_free_buffer(ptr: *mut c_uchar, len: c_int) {
    if !ptr.is_null() && len > 0 {
        unsafe {
            let vec = Vec::from_raw_parts(ptr, len as usize, len as usize);
            drop(vec);
        }
    }
}
