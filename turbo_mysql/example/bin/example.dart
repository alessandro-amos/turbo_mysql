import 'package:turbo_mysql/turbo_mysql.dart';

void main(List<String> arguments) async {
  final pool = MySqlPool(
    MySqlConfig(
      host: 'localhost',
      user: 'root',
      pass: 'password',
      port: 3306,
      dbName: 'test',
      poolMax: 10,
    ),
  );

  try {
    await pool.connect();

    final result = await pool.query('SELECT id, name FROM users LIMIT 10');

    for (final row in result.asMaps) {
      print('User: ${row['name']} (ID: ${row['id']})');
    }

    await pool.query(
      'INSERT INTO logs (message, created_at) VALUES (?, ?)',
      ['System started', DateTime.now()],
    );
  } finally {
    await pool.close();
  }
}
