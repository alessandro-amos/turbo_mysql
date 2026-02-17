import 'dart:async';
import 'dart:ffi';
import 'package:ffi/ffi.dart';
import 'bindings.dart';
import 'binary_io.dart';
import 'data_converter.dart';
import 'query_dispatcher.dart';
import 'mysql_config.dart';
import 'mysql_exception.dart';
import 'query_result.dart';
import 'mysql_connection.dart';
import 'prepared_statement.dart';

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
        handleQueryCallback,
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

    final (queryId, future) = registerQuery();

    return using((arena) {
      final queryPtr = sql.toNativeUtf8(allocator: arena);

      mysql_pool_query_raw(
        _poolPtr!,
        queryPtr,
        queryId,
        _callback!.nativeFunction,
      );
      return future;
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

    final (queryId, future) = registerQuery();

    return using((arena) {
      final queryPtr = sql.toNativeUtf8(allocator: arena);
      final writer = BinaryWriter();
      final paramsPtr = DataConverter.encodeParams(params, arena, writer);

      mysql_pool_query(
        _poolPtr!,
        queryPtr,
        paramsPtr,
        writer.toBytes().length,
        queryId,
        _callback!.nativeFunction,
      );
      return future;
    });
  }

  /// Prepares a SQL statement for repeated execution.
  Future<PreparedStatement> prepare(String sql) async {
    if (!_isInitialized || _poolPtr == null || _poolPtr == nullptr) {
      throw MySQLException('Not connected. Call connect() first.');
    }

    final (queryId, future) = registerQuery();

    return using((arena) {
      final queryPtr = sql.toNativeUtf8(allocator: arena);
      mysql_pool_prepare(
        _poolPtr!,
        queryPtr,
        queryId,
        _callback!.nativeFunction,
      );
      return future.then((res) {
        final ptrAddr = res.affectedRows;
        final ptr = Pointer<Void>.fromAddress(ptrAddr);
        return PreparedStatement(ptr, _callback!);
      });
    });
  }

  /// Starts a new transaction and returns a dedicated [MySqlConnection].
  Future<MySqlConnection> beginTransaction() async {
    if (!_isInitialized || _poolPtr == null || _poolPtr == nullptr) {
      throw MySQLException('Not connected. Call connect() first.');
    }

    final (queryId, future) = registerQuery();

    mysql_pool_begin_transaction(_poolPtr!, queryId, _callback!.nativeFunction);

    return future.then((res) {
      final ptrAddr = res.affectedRows;
      final ptr = Pointer<Void>.fromAddress(ptrAddr);
      return MySqlConnection(ptr, _callback!, isTransaction: true);
    });
  }

  /// Gets a dedicated connection from the pool without starting a transaction.
  Future<MySqlConnection> getConnection() async {
    if (!_isInitialized || _poolPtr == null || _poolPtr == nullptr) {
      throw MySQLException('Not connected. Call connect() first.');
    }

    final (queryId, future) = registerQuery();

    mysql_pool_get_connection(_poolPtr!, queryId, _callback!.nativeFunction);

    return future.then((res) {
      final ptrAddr = res.affectedRows;
      final ptr = Pointer<Void>.fromAddress(ptrAddr);
      return MySqlConnection(ptr, _callback!, isTransaction: false);
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

      return future.then((res) => res.affectedRows);
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

    clearPendingQueries();
  }

  /// Returns `true` if the pool is initialized and connected.
  bool get isConnected =>
      _isInitialized && _poolPtr != null && _poolPtr != nullptr;
}
