/// A high-performance MySQL client for Dart, powered by Rust.
///
/// This library provides a robust connection pool, prepared statements,
/// and efficient batch operations for MySQL and MariaDB databases.
///
/// ## Example
///
/// ```dart
/// import 'package:turbo_mysql/turbo_mysql.dart';
///
/// Future<void> main() async {
///   // Configure the connection pool
///   final pool = MySqlPool(
///     host: '127.0.0.1',
///     port: 3306,
///     user: 'root',
///     password: 'password',
///     db: 'my_database',
///     poolSize: 5,
///   );
///
///   try {
///     // Establish connection
///     await pool.connect();
///
///     // Execute a simple query
///     final result = await pool.query('SELECT id, name FROM users LIMIT 10');
///
///     for (final row in result.asMaps) {
///       print('User: ${row['name']} (ID: ${row['id']})');
///     }
///
///     // Execute with parameters
///     await pool.execute(
///       'INSERT INTO logs (message, created_at) VALUES (?, ?)',
///       ['System started', DateTime.now()],
///     );
///
///   } finally {
///     // Always close the pool to release resources
///     await pool.close();
///   }
/// }
/// ```
library;

export 'src/mysql_exception.dart';
export 'src/query_result.dart';
export 'src/mysql_config.dart';
export 'src/mysql_client.dart';
