import 'dart:async';
import 'dart:ffi';
import 'package:ffi/ffi.dart';
import 'bindings.dart';
import 'binary_io.dart';
import 'data_converter.dart';
import 'query_dispatcher.dart';
import 'mysql_exception.dart';
import 'query_result.dart';

/// A prepared statement that can be executed multiple times with different parameters.
class PreparedStatement {
  final Pointer<Void> _stmtPtr;
  final NativeCallable<QueryCallbackNative> _callback;
  bool _isClosed = false;

  PreparedStatement(this._stmtPtr, this._callback);

  /// Executes this prepared statement using the MySQL Binary Protocol with the given [params].
  Future<QueryResult> execute([List<dynamic> params = const []]) async {
    if (_isClosed) throw MySQLException('Statement is closed');

    final (queryId, future) = registerQuery();

    return using((arena) {
      final writer = BinaryWriter();
      final paramsPtr = DataConverter.encodeParams(params, arena, writer);

      mysql_stmt_execute(
        _stmtPtr,
        paramsPtr,
        writer.toBytes().length,
        queryId,
        _callback.nativeFunction,
      );
      return future;
    });
  }

  /// Destroys the statement and releases its resources.
  Future<void> release() async {
    if (_isClosed) return;
    mysql_stmt_destroy(_stmtPtr);
    _isClosed = true;
  }
}
