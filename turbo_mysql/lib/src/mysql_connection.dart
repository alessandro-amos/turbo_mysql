import 'dart:async';
import 'dart:ffi';
import 'package:ffi/ffi.dart';
import 'bindings.dart';
import 'binary_io.dart';
import 'data_converter.dart';
import 'query_dispatcher.dart';
import 'mysql_exception.dart';
import 'query_result.dart';

/// A dedicated connection to the database.
class MySqlConnection {
  final Pointer<Void> _connPtr;
  final NativeCallable<QueryCallbackNative> _callback;

  /// Indicates if this connection was started as part of a transaction.
  final bool isTransaction;

  bool _isClosed = false;

  MySqlConnection(
    this._connPtr,
    this._callback, {
    this.isTransaction = false,
  });

  /// Executes a raw SQL query using the MySQL Text Protocol.
  Future<QueryResult> queryRaw(String sql) async {
    if (_isClosed) throw MySQLException('Connection is closed');

    final (queryId, future) = registerQuery();

    return using((arena) {
      final queryPtr = sql.toNativeUtf8(allocator: arena);

      mysql_conn_query_raw(
        _connPtr,
        queryPtr,
        queryId,
        _callback.nativeFunction,
      );
      return future;
    });
  }

  /// Executes a parameterized SQL query using the MySQL Binary Protocol (Prepared Statements).
  Future<QueryResult> query(
    String sql, [
    List<dynamic> params = const [],
  ]) async {
    if (_isClosed) throw MySQLException('Connection is closed');

    final (queryId, future) = registerQuery();

    return using((arena) {
      final queryPtr = sql.toNativeUtf8(allocator: arena);
      final writer = BinaryWriter();
      final paramsPtr = DataConverter.encodeParams(params, arena, writer);

      mysql_conn_query(
        _connPtr,
        queryPtr,
        paramsPtr,
        writer.toBytes().length,
        queryId,
        _callback.nativeFunction,
      );
      return future;
    });
  }

  /// Commits the active transaction on this connection and releases it.
  Future<void> commit() async {
    if (_isClosed) throw MySQLException('Connection is closed');
    if (!isTransaction) throw MySQLException('Not a transaction connection');

    final (queryId, future) = registerQuery();

    try {
      mysql_conn_commit(_connPtr, queryId, _callback.nativeFunction);
      await future;
    } finally {
      await release();
    }
  }

  /// Rolls back the active transaction on this connection and releases it.
  Future<void> rollback() async {
    if (_isClosed) throw MySQLException('Connection is closed');
    if (!isTransaction) throw MySQLException('Not a transaction connection');

    final (queryId, future) = registerQuery();

    try {
      mysql_conn_rollback(_connPtr, queryId, _callback.nativeFunction);
      await future;
    } finally {
      await release();
    }
  }

  /// Performs a batch insert operation using this connection.
  Future<int> insertBatch(
    String table,
    List<String> columns,
    List<List<dynamic>> rows,
  ) async {
    return _executeBatch(table, columns, rows, false);
  }

  /// Performs a batch upsert operation (ON DUPLICATE KEY UPDATE) using this connection.
  Future<int> upsertBatch(
    String table,
    List<String> columns,
    List<List<dynamic>> rows,
  ) async {
    return _executeBatch(table, columns, rows, true);
  }

  Future<int> _executeBatch(
    String table,
    List<String> columns,
    List<List<dynamic>> rows,
    bool onDuplicate,
  ) async {
    if (_isClosed) throw MySQLException('Connection is closed');
    if (rows.isEmpty) return 0;
    if (columns.isEmpty) throw MySQLException('Columns must not be empty');

    for (final row in rows) {
      if (row.length != columns.length) {
        throw MySQLException('Row length does not match columns length');
      }
    }

    final (queryId, future) = registerQuery();

    return using((arena) {
      final tablePtr = table.toNativeUtf8(allocator: arena);
      final columnsPtr = columns.join(',').toNativeUtf8(allocator: arena);

      final writer = BinaryWriter();
      writer.writeUint32(rows.length);
      for (final row in rows) {
        for (final param in row) {
          DataConverter.writeParam(writer, param);
        }
      }

      final bytes = writer.toBytes();
      final ptr = arena.allocate<Uint8>(bytes.length);
      ptr.asTypedList(bytes.length).setAll(0, bytes);

      if (onDuplicate) {
        mysql_conn_batch_upsert(
          _connPtr,
          tablePtr,
          columnsPtr,
          ptr,
          bytes.length,
          queryId,
          _callback.nativeFunction,
        );
      } else {
        mysql_conn_batch_insert(
          _connPtr,
          tablePtr,
          columnsPtr,
          ptr,
          bytes.length,
          queryId,
          _callback.nativeFunction,
        );
      }

      return future.then((res) => res.affectedRows);
    });
  }

  /// Releases this connection back to the pool.
  Future<void> release() async {
    if (_isClosed) return;
    mysql_conn_destroy(_connPtr);
    _isClosed = true;
  }
}
