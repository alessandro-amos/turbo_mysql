import 'dart:ffi';
import 'package:ffi/ffi.dart';

/// Callback function signature for receiving query results from Rust.
typedef QueryCallbackNative =
    Void Function(Int64 id, Pointer<Uint8> data, Int32 len);

/// Creates a new MySQL connection pool in the Rust layer.
@Native<Pointer<Void> Function(Pointer<Utf8>)>(
  assetId: 'package:turbo_mysql/turbo_mysql_core',
)
external Pointer<Void> mysql_pool_create(Pointer<Utf8> url);

/// Destroys the connection pool and frees its resources.
@Native<Void Function(Pointer<Void>)>(
  assetId: 'package:turbo_mysql/turbo_mysql_core',
)
external void mysql_pool_destroy(Pointer<Void> pool);

/// Executes a raw text query on the pool using the MySQL Text Protocol.
@Native<
  Void Function(
    Pointer<Void>,
    Pointer<Utf8>,
    Int64,
    Pointer<NativeFunction<QueryCallbackNative>>,
  )
>(
  assetId: 'package:turbo_mysql/turbo_mysql_core',
)
external void mysql_pool_query_raw(
  Pointer<Void> pool,
  Pointer<Utf8> query,
  int id,
  Pointer<NativeFunction<QueryCallbackNative>> callback,
);

/// Executes a query with parameters on the pool using the MySQL Binary Protocol (Prepared Statements).
@Native<
  Void Function(
    Pointer<Void>,
    Pointer<Utf8>,
    Pointer<Uint8>,
    Int32,
    Int64,
    Pointer<NativeFunction<QueryCallbackNative>>,
  )
>(assetId: 'package:turbo_mysql/turbo_mysql_core')
external void mysql_pool_query(
  Pointer<Void> pool,
  Pointer<Utf8> query,
  Pointer<Uint8> params,
  int paramsLen,
  int id,
  Pointer<NativeFunction<QueryCallbackNative>> callback,
);

/// Prepares a statement on the pool.
@Native<
  Void Function(
    Pointer<Void>,
    Pointer<Utf8>,
    Int64,
    Pointer<NativeFunction<QueryCallbackNative>>,
  )
>(
  assetId: 'package:turbo_mysql/turbo_mysql_core',
)
external void mysql_pool_prepare(
  Pointer<Void> pool,
  Pointer<Utf8> query,
  int id,
  Pointer<NativeFunction<QueryCallbackNative>> callback,
);

/// Performs a batch insert operation on the pool.
@Native<
  Void Function(
    Pointer<Void>,
    Pointer<Utf8>,
    Pointer<Utf8>,
    Pointer<Uint8>,
    Int32,
    Int64,
    Pointer<NativeFunction<QueryCallbackNative>>,
  )
>(assetId: 'package:turbo_mysql/turbo_mysql_core')
external void mysql_pool_batch_insert(
  Pointer<Void> pool,
  Pointer<Utf8> table,
  Pointer<Utf8> columns,
  Pointer<Uint8> data,
  int dataLen,
  int id,
  Pointer<NativeFunction<QueryCallbackNative>> callback,
);

/// Performs a batch upsert (ON DUPLICATE KEY UPDATE) operation on the pool.
@Native<
  Void Function(
    Pointer<Void>,
    Pointer<Utf8>,
    Pointer<Utf8>,
    Pointer<Uint8>,
    Int32,
    Int64,
    Pointer<NativeFunction<QueryCallbackNative>>,
  )
>(assetId: 'package:turbo_mysql/turbo_mysql_core')
external void mysql_pool_batch_upsert(
  Pointer<Void> pool,
  Pointer<Utf8> table,
  Pointer<Utf8> columns,
  Pointer<Uint8> data,
  int dataLen,
  int id,
  Pointer<NativeFunction<QueryCallbackNative>> callback,
);

/// Starts a transaction and returns a dedicated connection.
@Native<
  Void Function(
    Pointer<Void>,
    Int64,
    Pointer<NativeFunction<QueryCallbackNative>>,
  )
>(
  assetId: 'package:turbo_mysql/turbo_mysql_core',
)
external void mysql_pool_begin_transaction(
  Pointer<Void> pool,
  int id,
  Pointer<NativeFunction<QueryCallbackNative>> callback,
);

/// Gets a dedicated connection from the pool without starting a transaction.
@Native<
  Void Function(
    Pointer<Void>,
    Int64,
    Pointer<NativeFunction<QueryCallbackNative>>,
  )
>(
  assetId: 'package:turbo_mysql/turbo_mysql_core',
)
external void mysql_pool_get_connection(
  Pointer<Void> pool,
  int id,
  Pointer<NativeFunction<QueryCallbackNative>> callback,
);

/// Executes a raw text query on a specific connection.
@Native<
  Void Function(
    Pointer<Void>,
    Pointer<Utf8>,
    Int64,
    Pointer<NativeFunction<QueryCallbackNative>>,
  )
>(
  assetId: 'package:turbo_mysql/turbo_mysql_core',
)
external void mysql_conn_query_raw(
  Pointer<Void> conn,
  Pointer<Utf8> query,
  int id,
  Pointer<NativeFunction<QueryCallbackNative>> callback,
);

/// Executes a query with parameters on a specific connection.
@Native<
  Void Function(
    Pointer<Void>,
    Pointer<Utf8>,
    Pointer<Uint8>,
    Int32,
    Int64,
    Pointer<NativeFunction<QueryCallbackNative>>,
  )
>(assetId: 'package:turbo_mysql/turbo_mysql_core')
external void mysql_conn_query(
  Pointer<Void> conn,
  Pointer<Utf8> query,
  Pointer<Uint8> params,
  int paramsLen,
  int id,
  Pointer<NativeFunction<QueryCallbackNative>> callback,
);

/// Commits the transaction on the connection.
@Native<
  Void Function(
    Pointer<Void>,
    Int64,
    Pointer<NativeFunction<QueryCallbackNative>>,
  )
>(
  assetId: 'package:turbo_mysql/turbo_mysql_core',
)
external void mysql_conn_commit(
  Pointer<Void> conn,
  int id,
  Pointer<NativeFunction<QueryCallbackNative>> callback,
);

/// Rolls back the transaction on the connection.
@Native<
  Void Function(
    Pointer<Void>,
    Int64,
    Pointer<NativeFunction<QueryCallbackNative>>,
  )
>(
  assetId: 'package:turbo_mysql/turbo_mysql_core',
)
external void mysql_conn_rollback(
  Pointer<Void> conn,
  int id,
  Pointer<NativeFunction<QueryCallbackNative>> callback,
);

/// Performs a batch insert operation on a specific connection.
@Native<
  Void Function(
    Pointer<Void>,
    Pointer<Utf8>,
    Pointer<Utf8>,
    Pointer<Uint8>,
    Int32,
    Int64,
    Pointer<NativeFunction<QueryCallbackNative>>,
  )
>(assetId: 'package:turbo_mysql/turbo_mysql_core')
external void mysql_conn_batch_insert(
  Pointer<Void> conn,
  Pointer<Utf8> table,
  Pointer<Utf8> columns,
  Pointer<Uint8> data,
  int dataLen,
  int id,
  Pointer<NativeFunction<QueryCallbackNative>> callback,
);

/// Performs a batch upsert (ON DUPLICATE KEY UPDATE) logic on a specific connection.
@Native<
  Void Function(
    Pointer<Void>,
    Pointer<Utf8>,
    Pointer<Utf8>,
    Pointer<Uint8>,
    Int32,
    Int64,
    Pointer<NativeFunction<QueryCallbackNative>>,
  )
>(assetId: 'package:turbo_mysql/turbo_mysql_core')
external void mysql_conn_batch_upsert(
  Pointer<Void> conn,
  Pointer<Utf8> table,
  Pointer<Utf8> columns,
  Pointer<Uint8> data,
  int dataLen,
  int id,
  Pointer<NativeFunction<QueryCallbackNative>> callback,
);

/// Destroys a connection (internal use).
@Native<Void Function(Pointer<Void>)>(
  assetId: 'package:turbo_mysql/turbo_mysql_core',
)
external void mysql_conn_destroy(Pointer<Void> conn);

/// Executes a prepared statement with parameters using the MySQL Binary Protocol.
@Native<
  Void Function(
    Pointer<Void>,
    Pointer<Uint8>,
    Int32,
    Int64,
    Pointer<NativeFunction<QueryCallbackNative>>,
  )
>(
  assetId: 'package:turbo_mysql/turbo_mysql_core',
)
external void mysql_stmt_execute(
  Pointer<Void> stmt,
  Pointer<Uint8> params,
  int paramsLen,
  int id,
  Pointer<NativeFunction<QueryCallbackNative>> callback,
);

/// Destroys a prepared statement and frees its resources.
@Native<Void Function(Pointer<Void>)>(
  assetId: 'package:turbo_mysql/turbo_mysql_core',
)
external void mysql_stmt_destroy(Pointer<Void> stmt);

/// Frees a buffer allocated by the Rust side.
@Native<Void Function(Pointer<Uint8>, Int32)>(
  assetId: 'package:turbo_mysql/turbo_mysql_core',
)
external void mysql_buffer_free(Pointer<Uint8> ptr, int len);
