import 'dart:async';
import 'dart:ffi';
import 'bindings.dart';
import 'binary_io.dart';
import 'data_converter.dart';
import 'mysql_exception.dart';
import 'query_result.dart';

/// Global map to track pending queries by their unique ID.
final Map<int, Completer<dynamic>> _pendingQueries = {};
int _nextQueryId = 1;

/// Registers a new pending query and returns its ID and Future.
(int, Future<QueryResult>) registerQuery() {
  final id = _nextQueryId++;
  final completer = Completer<QueryResult>();
  _pendingQueries[id] = completer;
  return (id, completer.future);
}

/// Registers a transaction or specialized operation that returns void/affected rows
/// but is treated generically in the pipeline.
(int, Future<T>) registerOp<T>() {
  final id = _nextQueryId++;
  final completer = Completer<T>();
  _pendingQueries[id] = completer;
  return (id, completer.future);
}

/// Global callback function invoked by Rust when a query completes.
void handleQueryCallback(int id, Pointer<Uint8> dataPtr, int len) {
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
      final colTypes = <int>[];
      final charsets = <int>[];

      for (var i = 0; i < colCount; i++) {
        columns.add(reader.readString());
        colTypes.add(reader.readUint16());
        charsets.add(reader.readUint16());
      }

      final rowCount = reader.readUint32();
      final rows = List<List<dynamic>>.generate(rowCount, (_) {
        return List<dynamic>.generate(colCount, (i) {
          final isPresent = reader.readUint8() == 1;
          if (!isPresent) return null;
          final bytes = reader.readBlob();
          return DataConverter.decodeValue(bytes, colTypes[i], charsets[i]);
        }, growable: false);
      }, growable: false);

      final result = QueryResult(
        columns: columns,
        rows: rows,
        affectedRows: affectedRows,
        lastInsertId: lastInsertId,
      );

      completer.complete(result);
    }
  } catch (e) {
    completer.completeError(
      MySQLException('Failed to parse binary result: $e'),
    );
  } finally {
    mysql_buffer_free(dataPtr, len);
  }
}

/// Clears all pending queries with an error (used when pool closes).
void clearPendingQueries() {
  for (final completer in _pendingQueries.values) {
    if (!completer.isCompleted) {
      completer.completeError(MySQLException('Connection closed'));
    }
  }
  _pendingQueries.clear();
}
