import 'dart:async';
import 'dart:convert';
import 'dart:ffi';
import 'dart:typed_data';
import 'package:ffi/ffi.dart';
import 'bindings.dart';
import 'binary_utils.dart';
import 'mysql_exception.dart';
import 'query_result.dart';
import 'mysql_config.dart';

final Map<int, Completer<dynamic>> _pendingQueries = {};
int _nextQueryId = 1;

void _handleQueryCallback(int id, Pointer<Uint8> dataPtr, int len) {
  final completer = _pendingQueries.remove(id);

  if (completer == null) {
    mysql_buffer_free(dataPtr, len);
    return;
  }

  try {
    final reader = BinaryReader(dataPtr, len);
    final status = reader.readUint8();

    if (status == 0) {
      final msg = reader.readString();
      completer.completeError(MySQLException(msg));
    } else {
      final affectedRows = reader.readUint64();
      final lastInsertId = reader.readUint64();
      final colCount = reader.readUint32();

      final columns = <String>[];
      for (var i = 0; i < colCount; i++) {
        columns.add(reader.readString());
      }

      final rowCount = reader.readUint32();
      final rows = List<List<dynamic>>.generate(rowCount, (_) {
        return List<dynamic>.generate(colCount, (_) {
          final type = reader.readUint8();
          switch (type) {
            case 0:
              return null;
            case 1:
              return reader.readInt64();
            case 2:
              return reader.readFloat64();
            case 3:
              final bytes = reader.readBlob();
              try {
                return utf8.decode(bytes);
              } catch (_) {
                return bytes;
              }
            case 4:
              return reader.readBlob();
            default:
              return null;
          }
        }, growable: false);
      }, growable: false);

      completer.complete(
        QueryResult(
          columns: columns,
          rows: rows,
          affectedRows: affectedRows,
          lastInsertId: lastInsertId,
        ),
      );
    }
  } catch (e) {
    completer.completeError(
      MySQLException('Failed to parse binary result: $e'),
    );
  } finally {
    mysql_buffer_free(dataPtr, len);
  }
}

Pointer<Uint8> _encodeParams(
  List<dynamic> params,
  Arena arena,
  BinaryWriter writer,
) {
  writer.writeUint32(params.length);
  for (final param in params) {
    if (param == null) {
      writer.writeUint8(0);
    } else if (param is int) {
      writer.writeUint8(1);
      writer.writeInt64(param);
    } else if (param is double) {
      writer.writeUint8(2);
      writer.writeFloat64(param);
    } else if (param is bool) {
      writer.writeUint8(1);
      writer.writeInt64(param ? 1 : 0);
    } else if (param is DateTime) {
      writer.writeUint8(4);
      final str = param.toIso8601String().replaceAll('T', ' ').substring(0, 19);
      writer.writeString(str);
    } else if (param is String) {
      writer.writeUint8(3);
      writer.writeString(param);
    } else if (param is Uint8List) {
      writer.writeUint8(4);
      writer.writeBlob(param);
    } else if (param is List<int>) {
      writer.writeUint8(4);
      writer.writeBlob(param);
    } else {
      writer.writeUint8(3);
      writer.writeString(param.toString());
    }
  }
  final bytes = writer.toBytes();
  final ptr = arena.allocate<Uint8>(bytes.length);
  ptr.asTypedList(bytes.length).setAll(0, bytes);
  return ptr;
}

/// A prepared statement that can be executed multiple times with different parameters.
class PreparedStatement {
  final Pointer<Void> _stmtPtr;
  final NativeCallable<QueryCallbackNative> _callback;
  bool _isClosed = false;

  PreparedStatement._(this._stmtPtr, this._callback);

  /// Executes this prepared statement using the MySQL Binary Protocol with the given [params].
  Future<QueryResult> execute([List<dynamic> params = const []]) async {
    if (_isClosed) throw MySQLException('Statement is closed');

    final queryId = _nextQueryId++;
    final completer = Completer<QueryResult>();
    _pendingQueries[queryId] = completer;

    return using((arena) {
      final writer = BinaryWriter();
      final paramsPtr = _encodeParams(params, arena, writer);

      mysql_stmt_execute(
        _stmtPtr,
        paramsPtr,
        writer.toBytes().length,
        queryId,
        _callback.nativeFunction,
      );
      return completer.future;
    });
  }

  /// Destroys the statement and releases its resources.
  Future<void> release() async {
    if (_isClosed) return;
    mysql_stmt_destroy(_stmtPtr);
    _isClosed = true;
  }
}

/// A dedicated connection to the database, generally used for transactions.
class MySqlConnection {
  final Pointer<Void> _connPtr;
  final NativeCallable<QueryCallbackNative> _callback;
  bool _isClosed = false;

  MySqlConnection._(this._connPtr, this._callback);

  /// Executes a raw SQL query using the MySQL Text Protocol.
  Future<QueryResult> queryRaw(String sql) async {
    if (_isClosed) throw MySQLException('Connection is closed');

    final queryId = _nextQueryId++;
    final completer = Completer<QueryResult>();
    _pendingQueries[queryId] = completer;

    return using((arena) {
      final queryPtr = sql.toNativeUtf8(allocator: arena);

      mysql_conn_query_raw(
        _connPtr,
        queryPtr,
        queryId,
        _callback.nativeFunction,
      );
      return completer.future;
    });
  }

  /// Executes a parameterized SQL query using the MySQL Binary Protocol (Prepared Statements).
  Future<QueryResult> query(
    String sql, [
    List<dynamic> params = const [],
  ]) async {
    if (_isClosed) throw MySQLException('Connection is closed');

    final queryId = _nextQueryId++;
    final completer = Completer<QueryResult>();
    _pendingQueries[queryId] = completer;

    return using((arena) {
      final queryPtr = sql.toNativeUtf8(allocator: arena);
      final writer = BinaryWriter();
      final paramsPtr = _encodeParams(params, arena, writer);

      mysql_conn_query(
        _connPtr,
        queryPtr,
        paramsPtr,
        writer.toBytes().length,
        queryId,
        _callback.nativeFunction,
      );
      return completer.future;
    });
  }

  /// Commits the active transaction on this connection and releases it.
  Future<void> commit() async {
    if (_isClosed) throw MySQLException('Connection is closed');

    final queryId = _nextQueryId++;
    final completer = Completer<QueryResult>();
    _pendingQueries[queryId] = completer;

    try {
      mysql_conn_commit(_connPtr, queryId, _callback.nativeFunction);
      await completer.future;
    } finally {
      await release();
    }
  }

  /// Rolls back the active transaction on this connection and releases it.
  Future<void> rollback() async {
    if (_isClosed) throw MySQLException('Connection is closed');

    final queryId = _nextQueryId++;
    final completer = Completer<QueryResult>();
    _pendingQueries[queryId] = completer;

    try {
      mysql_conn_rollback(_connPtr, queryId, _callback.nativeFunction);

      await completer.future;
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

    final queryId = _nextQueryId++;
    final completer = Completer<QueryResult>();
    _pendingQueries[queryId] = completer;

    return using((arena) {
      final tablePtr = table.toNativeUtf8(allocator: arena);
      final columnsPtr = columns.join(',').toNativeUtf8(allocator: arena);

      final writer = BinaryWriter();
      writer.writeUint32(rows.length);
      for (final row in rows) {
        for (final param in row) {
          if (param == null) {
            writer.writeUint8(0);
          } else if (param is int) {
            writer.writeUint8(1);
            writer.writeInt64(param);
          } else if (param is double) {
            writer.writeUint8(2);
            writer.writeFloat64(param);
          } else if (param is bool) {
            writer.writeUint8(1);
            writer.writeInt64(param ? 1 : 0);
          } else if (param is DateTime) {
            writer.writeUint8(4);
            final str = param.toIso8601String().replaceAll('T', ' ').substring(0, 19);
            writer.writeString(str);
          } else if (param is String) {
            writer.writeUint8(3);
            writer.writeString(param);
          } else if (param is Uint8List) {
            writer.writeUint8(4);
            writer.writeBlob(param);
          } else if (param is List<int>) {
            writer.writeUint8(4);
            writer.writeBlob(param);
          } else {
            writer.writeUint8(3);
            writer.writeString(param.toString());
          }
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

      return completer.future.then((res) => res.affectedRows);
    });
  }

  /// Releases this connection back to the pool without committing or rolling back.
  Future<void> release() async {
    if (_isClosed) return;
    mysql_conn_destroy(_connPtr);
    _isClosed = true;
  }
}

/// A pool of MySQL connections for executing queries and managing transactions.
class MySqlPool {
  /// The configuration used to create this connection pool.
  final MySqlConfig config;

  Pointer<Void>? _poolPtr;
  NativeCallable<QueryCallbackNative>? _callback;
  bool _isInitialized = false;

  /// Creates a new [MySqlPool] instance with the specified [config].
  MySqlPool(this.config);

  /// Initializes the connection pool and connects to the database.
  Future<void> connect() async {
    if (_isInitialized) {
      throw MySQLException('Already connected');
    }

    final urlStr = config.toConnectionString();
    final urlNative = urlStr.toNativeUtf8();
    try {
      _poolPtr = mysql_pool_create(urlNative);

      if (_poolPtr == null || _poolPtr == nullptr) {
        throw MySQLException('Failed to create MySQL pool');
      }

      _callback = NativeCallable<QueryCallbackNative>.listener(
        _handleQueryCallback,
      );

      _isInitialized = true;

      try {
        await queryRaw('SELECT 1');
      } catch (e) {
        await close();
        rethrow;
      }
    } finally {
      malloc.free(urlNative);
    }
  }

  /// Ensures that the configured database exists, creating it if it does not.
  Future<void> ensureDatabase() async {
    final tempConfig = config.copyWith(dbName: '');
    final tempPool = MySqlPool(tempConfig);

    try {
      await tempPool.connect();
      await tempPool.queryRaw(
        'CREATE DATABASE IF NOT EXISTS `${config.dbName}`',
      );
    } finally {
      await tempPool.close();
    }
  }

  /// Executes a raw SQL query using the MySQL Text Protocol.
  Future<QueryResult> queryRaw(String sql) async {
    if (!_isInitialized || _poolPtr == null || _poolPtr == nullptr) {
      throw MySQLException('Not connected. Call connect() first.');
    }

    final queryId = _nextQueryId++;
    final completer = Completer<QueryResult>();
    _pendingQueries[queryId] = completer;

    return using((arena) {
      final queryPtr = sql.toNativeUtf8(allocator: arena);

      mysql_pool_query_raw(
        _poolPtr!,
        queryPtr,
        queryId,
        _callback!.nativeFunction,
      );
      return completer.future;
    });
  }

  /// Executes a parameterized SQL query using the MySQL Binary Protocol (Prepared Statements).
  Future<QueryResult> query(
    String sql, [
    List<dynamic> params = const [],
  ]) async {
    if (!_isInitialized || _poolPtr == null || _poolPtr == nullptr) {
      throw MySQLException('Not connected. Call connect() first.');
    }

    final queryId = _nextQueryId++;
    final completer = Completer<QueryResult>();
    _pendingQueries[queryId] = completer;

    return using((arena) {
      final queryPtr = sql.toNativeUtf8(allocator: arena);
      final writer = BinaryWriter();
      final paramsPtr = _encodeParams(params, arena, writer);

      mysql_pool_query(
        _poolPtr!,
        queryPtr,
        paramsPtr,
        writer.toBytes().length,
        queryId,
        _callback!.nativeFunction,
      );
      return completer.future;
    });
  }

  /// Prepares a SQL statement for repeated execution.
  Future<PreparedStatement> prepare(String sql) async {
    if (!_isInitialized || _poolPtr == null || _poolPtr == nullptr) {
      throw MySQLException('Not connected. Call connect() first.');
    }

    final queryId = _nextQueryId++;
    final completer = Completer<QueryResult>();
    _pendingQueries[queryId] = completer;

    return using((arena) {
      final queryPtr = sql.toNativeUtf8(allocator: arena);
      mysql_pool_prepare(
        _poolPtr!,
        queryPtr,
        queryId,
        _callback!.nativeFunction,
      );
      return completer.future.then((res) {
        final ptrAddr = res.affectedRows;
        final ptr = Pointer<Void>.fromAddress(ptrAddr);
        return PreparedStatement._(ptr, _callback!);
      });
    });
  }

  /// Starts a new transaction and returns a dedicated [MySqlConnection].
  Future<MySqlConnection> beginTransaction() async {
    if (!_isInitialized || _poolPtr == null || _poolPtr == nullptr) {
      throw MySQLException('Not connected. Call connect() first.');
    }

    final queryId = _nextQueryId++;
    final completer = Completer<QueryResult>();
    _pendingQueries[queryId] = completer;

    mysql_pool_begin_transaction(_poolPtr!, queryId, _callback!.nativeFunction);

    return completer.future.then((res) {
      final ptrAddr = res.affectedRows;
      final ptr = Pointer<Void>.fromAddress(ptrAddr);
      return MySqlConnection._(ptr, _callback!);
    });
  }

  /// Performs a batch insert operation using a connection from the pool.
  Future<int> insertBatch(
    String table,
    List<String> columns,
    List<List<dynamic>> rows,
  ) async {
    return _executeBatch(table, columns, rows, false);
  }

  /// Performs a batch upsert operation (ON DUPLICATE KEY UPDATE) using a connection from the pool.
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
    if (!_isInitialized || _poolPtr == null || _poolPtr == nullptr) {
      throw MySQLException('Not connected. Call connect() first.');
    }
    if (rows.isEmpty) return 0;
    if (columns.isEmpty) throw MySQLException('Columns must not be empty');

    for (final row in rows) {
      if (row.length != columns.length) {
        throw MySQLException('Row length does not match columns length');
      }
    }

    final queryId = _nextQueryId++;
    final completer = Completer<QueryResult>();
    _pendingQueries[queryId] = completer;

    return using((arena) {
      final tablePtr = table.toNativeUtf8(allocator: arena);
      final columnsPtr = columns.join(',').toNativeUtf8(allocator: arena);

      final writer = BinaryWriter();
      writer.writeUint32(rows.length);
      for (final row in rows) {
        for (final param in row) {
          if (param == null) {
            writer.writeUint8(0);
          } else if (param is int) {
            writer.writeUint8(1);
            writer.writeInt64(param);
          } else if (param is double) {
            writer.writeUint8(2);
            writer.writeFloat64(param);
          } else if (param is bool) {
            writer.writeUint8(1);
            writer.writeInt64(param ? 1 : 0);
          } else if (param is DateTime) {
            writer.writeUint8(4);
            final str = param.toIso8601String().replaceAll('T', ' ').substring(0, 19);
            writer.writeString(str);
          } else if (param is String) {
            writer.writeUint8(3);
            writer.writeString(param);
          } else if (param is Uint8List) {
            writer.writeUint8(4);
            writer.writeBlob(param);
          } else if (param is List<int>) {
            writer.writeUint8(4);
            writer.writeBlob(param);
          } else {
            writer.writeUint8(3);
            writer.writeString(param.toString());
          }
        }
      }

      final bytes = writer.toBytes();
      final ptr = arena.allocate<Uint8>(bytes.length);
      ptr.asTypedList(bytes.length).setAll(0, bytes);

      if (onDuplicate) {
        mysql_pool_batch_upsert(
          _poolPtr!,
          tablePtr,
          columnsPtr,
          ptr,
          bytes.length,
          queryId,
          _callback!.nativeFunction,
        );
      } else {
        mysql_pool_batch_insert(
          _poolPtr!,
          tablePtr,
          columnsPtr,
          ptr,
          bytes.length,
          queryId,
          _callback!.nativeFunction,
        );
      }

      return completer.future.then((res) => res.affectedRows);
    });
  }

  /// Closes the connection pool and releases all underlying resources.
  Future<void> close() async {
    if (!_isInitialized) return;

    _callback?.close();
    _callback = null;

    if (_poolPtr != null && _poolPtr != nullptr) {
      mysql_pool_destroy(_poolPtr!);
      _poolPtr = null;
    }

    _isInitialized = false;

    for (final completer in _pendingQueries.values) {
      if (!completer.isCompleted) {
        completer.completeError(MySQLException('Connection closed'));
      }
    }
    _pendingQueries.clear();
  }

  /// Returns `true` if the pool is initialized and connected.
  bool get isConnected => _isInitialized && _poolPtr != null && _poolPtr != nullptr;
}
