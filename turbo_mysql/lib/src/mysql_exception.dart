/// Represents a generic exception that occurred during a MySQL operation.
class MySQLException implements Exception {
  /// The error message returned by the operation.
  final String message;

  /// Creates a new [MySQLException] with the given [message].
  MySQLException(this.message);

  @override
  String toString() => 'MySQLException: $message';
}
