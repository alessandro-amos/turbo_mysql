/// Represents the result of a MySQL query execution.
class QueryResult {
  /// The list of column names returned by the query.
  final List<String> columns;

  /// The data rows, where each row is a list of values in the order of [columns].
  final List<List<dynamic>> rows;

  /// The number of rows affected by the query (for INSERT, UPDATE, DELETE).
  final int affectedRows;

  /// The auto-generated ID from the last INSERT operation.
  final int lastInsertId;

  /// Creates a [QueryResult] with the given data.
  QueryResult({
    required this.columns,
    required this.rows,
    required this.affectedRows,
    required this.lastInsertId,
  });

  /// Returns the rows as a list of maps.
  ///
  /// Each map contains keys corresponding to the column names.
  List<Map<String, dynamic>> get asMaps {
    if (columns.isEmpty || rows.isEmpty) return const [];
    return rows.map((row) {
      final map = <String, dynamic>{};
      for (var i = 0; i < columns.length; i++) {
        if (i < row.length) {
          map[columns[i]] = row[i];
        }
      }
      return map;
    }).toList();
  }

  @override
  String toString() =>
      'QueryResult(rows: ${rows.length}, affected: $affectedRows, id: $lastInsertId)';
}
