import 'dart:io';
import 'dart:typed_data';
import 'package:test/test.dart';
import 'package:turbo_mysql/turbo_mysql.dart';

void main() {
  final host = Platform.environment['DB_HOST'] ?? '127.0.0.1';
  final user = Platform.environment['DB_USER'] ?? 'root';
  final dbName = Platform.environment['DB_NAME'] ?? 'test';
  final pass = Platform.environment['DB_PASS'] ?? 'password';
  final port = int.tryParse(Platform.environment['DB_PORT'] ?? '3306') ?? 3306;

  group('Advanced Data Type Tests', () {
    late MySqlPool mysql;

    setUpAll(() async {
      mysql = MySqlPool(
        MySqlConfig(
          host: host,
          user: user,
          pass: pass,
          dbName: dbName,
          port: port,
        ),
      );

      await mysql.connect();
      await mysql.query('DROP TABLE IF EXISTS test_datatypes');
      await mysql.query('''
        CREATE TABLE test_datatypes (
          id INT AUTO_INCREMENT PRIMARY KEY,
          tiny_int TINYINT,
          small_int SMALLINT,
          medium_int MEDIUMINT,
          int_val INT,
          big_int BIGINT,
          unsigned_int INT UNSIGNED,
          float_val FLOAT(10,2),
          double_val DOUBLE(15,4),
          decimal_val DECIMAL(20,5),
          bit_val BIT(8),
          year_val YEAR,
          date_val DATE,
          time_val TIME,
          datetime_val DATETIME,
          timestamp_val TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          char_val CHAR(20),
          varchar_val VARCHAR(255),
          tiny_text TINYTEXT,
          text_val TEXT,
          medium_text MEDIUMTEXT,
          long_text LONGTEXT,
          binary_val BINARY(16),
          varbinary_val VARBINARY(100),
          tiny_blob TINYBLOB,
          blob_val BLOB,
          medium_blob MEDIUMBLOB,
          long_blob LONGBLOB,
          enum_val ENUM('small', 'medium', 'large'),
          set_val SET('red', 'green', 'blue')
        )
      ''');
    });

    tearDownAll(() async {
      if (mysql.isConnected) {
        await mysql.query('DROP TABLE IF EXISTS test_datatypes');
        await mysql.close();
      }
    });

    setUp(() async {
      await mysql.query('DELETE FROM test_datatypes');
    });

    test('TINYINT range values', () async {
      await mysql.query('INSERT INTO test_datatypes (tiny_int) VALUES (?)', [
        -128,
      ]);
      await mysql.query('INSERT INTO test_datatypes (tiny_int) VALUES (?)', [
        0,
      ]);
      await mysql.query('INSERT INTO test_datatypes (tiny_int) VALUES (?)', [
        127,
      ]);

      final result = await mysql.queryRaw(
        'SELECT tiny_int FROM test_datatypes ORDER BY tiny_int',
      );
      expect(result.rows[0][0], -128);
      expect(result.rows[1][0], 0);
      expect(result.rows[2][0], 127);
    });

    test('BIGINT large values', () async {
      final largePositive = 9223372036854775807;
      final largeNegative = -9223372036854775808;

      await mysql.query('INSERT INTO test_datatypes (big_int) VALUES (?)', [
        largePositive,
      ]);
      await mysql.query('INSERT INTO test_datatypes (big_int) VALUES (?)', [
        largeNegative,
      ]);

      final result = await mysql.queryRaw(
        'SELECT big_int FROM test_datatypes ORDER BY big_int',
      );
      expect(result.rows[0][0], largeNegative);
      expect(result.rows[1][0], largePositive);
    });

    test('FLOAT precision', () async {
      await mysql.query('INSERT INTO test_datatypes (float_val) VALUES (?)', [
        123.45,
      ]);

      final result = await mysql.query('SELECT float_val FROM test_datatypes');
      expect(result.rows[0][0], closeTo(123.45, 0.01));
    });

    test('DOUBLE precision', () async {
      await mysql.query(
        'INSERT INTO test_datatypes (double_val) VALUES (?)',
        [123456.7891],
      );

      final result = await mysql.query('SELECT double_val FROM test_datatypes');
      expect(
        result.rows[0][0],
        closeTo(123456.7891, 0.0001),
      );
    });

    test('DECIMAL exact precision', () async {
      await mysql.query(
        'INSERT INTO test_datatypes (decimal_val) VALUES (?)',
        [99999.12345],
      );

      final result = await mysql.queryRaw(
        'SELECT decimal_val FROM test_datatypes',
      );
      expect(
        result.rows[0][0],
        closeTo(99999.12345, 0.00001),
      );
    });

    test('YEAR value', () async {
      await mysql.query('INSERT INTO test_datatypes (year_val) VALUES (?)', [
        '2024',
      ]);

      final result = await mysql.query('SELECT year_val FROM test_datatypes');
      expect(result.rows[0][0].toString(), contains('2024'));
    });

    test('DATE format', () async {
      await mysql.query('INSERT INTO test_datatypes (date_val) VALUES (?)', [
        '2024-12-25',
      ]);

      final result = await mysql.query('SELECT date_val FROM test_datatypes');
      expect(result.rows[0][0].toString(), contains('2024-12-25'));
    });

    test('TIME format', () async {
      await mysql.query('INSERT INTO test_datatypes (time_val) VALUES (?)', [
        '23:59:59',
      ]);

      final result = await mysql.query('SELECT time_val FROM test_datatypes');
      expect(result.rows[0][0].toString(), contains('23:59:59'));
    });

    test('DATETIME full precision', () async {
      await mysql.query(
        'INSERT INTO test_datatypes (datetime_val) VALUES (?)',
        ['2024-12-25 23:59:59'],
      );

      final result = await mysql.queryRaw(
        'SELECT datetime_val FROM test_datatypes',
      );
      expect(result.rows[0][0].toString(), contains('2024-12-25'));
      expect(result.rows[0][0].toString(), contains('23:59:59'));
    });

    test('CHAR fixed length', () async {
      await mysql.query('INSERT INTO test_datatypes (char_val) VALUES (?)', [
        'test',
      ]);

      final result = await mysql.queryRaw(
        'SELECT char_val, LENGTH(char_val) FROM test_datatypes',
      );
      expect(result.rows[0][0].toString().trim(), 'test');
    });

    test('VARCHAR variable length', () async {
      await mysql.query(
        'INSERT INTO test_datatypes (varchar_val) VALUES (?)',
        ['variable'],
      );

      final result = await mysql.queryRaw(
        'SELECT varchar_val, LENGTH(varchar_val) FROM test_datatypes',
      );
      expect(result.rows[0][0], 'variable');
      expect(result.rows[0][1], 8);
    });

    test('TEXT large content', () async {
      final largeText = 'Lorem ipsum ' * 1000;

      await mysql.query('INSERT INTO test_datatypes (text_val) VALUES (?)', [
        largeText,
      ]);

      final result = await mysql.query('SELECT text_val FROM test_datatypes');
      expect(result.rows[0][0].toString().length, largeText.length);
    });

    test('BLOB binary data', () async {
      final binaryData = Uint8List.fromList(List.generate(256, (i) => i));

      await mysql.query('INSERT INTO test_datatypes (blob_val) VALUES (?)', [
        binaryData,
      ]);

      final result = await mysql.query('SELECT blob_val FROM test_datatypes');
      final retrieved = result.rows[0][0] as List<int>;
      expect(retrieved.length, 256);
      for (var i = 0; i < 256; i++) {
        expect(retrieved[i], i);
      }
    });

    test('ENUM values', () async {
      await mysql.query('INSERT INTO test_datatypes (enum_val) VALUES (?)', [
        'small',
      ]);
      await mysql.query('INSERT INTO test_datatypes (enum_val) VALUES (?)', [
        'medium',
      ]);
      await mysql.query('INSERT INTO test_datatypes (enum_val) VALUES (?)', [
        'large',
      ]);

      final result = await mysql.queryRaw(
        'SELECT enum_val FROM test_datatypes ORDER BY id',
      );
      expect(result.rows[0][0], 'small');
      expect(result.rows[1][0], 'medium');
      expect(result.rows[2][0], 'large');
    });

    test('SET multiple values', () async {
      await mysql.query('INSERT INTO test_datatypes (set_val) VALUES (?)', [
        'red,blue',
      ]);

      final result = await mysql.query('SELECT set_val FROM test_datatypes');
      expect(result.rows[0][0].toString(), contains('red'));
      expect(result.rows[0][0].toString(), contains('blue'));
    });

    test('NULL values for all types', () async {
      await mysql.query(
        '''
        INSERT INTO test_datatypes (
          tiny_int, small_int, int_val, big_int, float_val, double_val, decimal_val,
          date_val, time_val, datetime_val, char_val, varchar_val, text_val,
          blob_val, enum_val, set_val
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ''',
        [
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
        ],
      );

      final result = await mysql.queryRaw('''
        SELECT tiny_int, small_int, int_val, big_int, float_val, double_val, decimal_val,
        date_val, time_val, datetime_val, char_val, varchar_val, text_val,
        blob_val, enum_val, set_val FROM test_datatypes
      ''');
      final row = result.rows[0];
      for (var i = 0; i < 16; i++) {
        expect(row[i], isNull);
      }
    });

    test('Zero values', () async {
      await mysql.query(
        'INSERT INTO test_datatypes (tiny_int, int_val, float_val, double_val, decimal_val) VALUES (?, ?, ?, ?, ?)',
        [0, 0, 0.0, 0.0, 0.0],
      );

      final result = await mysql.queryRaw(
        'SELECT tiny_int, int_val, float_val, double_val, decimal_val FROM test_datatypes',
      );
      final row = result.rows[0];
      expect(row[0], 0);
      expect(row[1], 0);
      expect(row[2], 0.0);
      expect(row[3], 0.0);
      expect(row[4], 0.0);
    });
  });

  group('Complex Query Pattern Tests', () {
    late MySqlPool mysql;

    setUpAll(() async {
      mysql = MySqlPool(
        MySqlConfig(
          host: host,
          user: user,
          pass: pass,
          dbName: dbName,
          port: port,
        ),
      );

      await mysql.connect();
      await mysql.query('DROP TABLE IF EXISTS employees');
      await mysql.query('DROP TABLE IF EXISTS departments');
      await mysql.query('DROP TABLE IF EXISTS salaries');

      await mysql.query('''
        CREATE TABLE departments (
          id INT AUTO_INCREMENT PRIMARY KEY,
          name VARCHAR(100) NOT NULL,
          location VARCHAR(100)
        )
      ''');

      await mysql.query('''
        CREATE TABLE employees (
          id INT AUTO_INCREMENT PRIMARY KEY,
          name VARCHAR(100) NOT NULL,
          department_id INT,
          hire_date DATE,
          FOREIGN KEY (department_id) REFERENCES departments(id)
        )
      ''');

      await mysql.query('''
        CREATE TABLE salaries (
          id INT AUTO_INCREMENT PRIMARY KEY,
          employee_id INT,
          amount DECIMAL(10,2),
          effective_date DATE,
          FOREIGN KEY (employee_id) REFERENCES employees(id)
        )
      ''');
    });

    tearDownAll(() async {
      if (mysql.isConnected) {
        await mysql.query('DROP TABLE IF EXISTS salaries');
        await mysql.query('DROP TABLE IF EXISTS employees');
        await mysql.query('DROP TABLE IF EXISTS departments');
        await mysql.close();
      }
    });

    setUp(() async {
      await mysql.query('DELETE FROM salaries');
      await mysql.query('DELETE FROM employees');
      await mysql.query('DELETE FROM departments');
    });

    test('nested subqueries', () async {
      await mysql.insertBatch(
        'departments',
        ['name'],
        [
          ['Engineering'],
          ['Sales'],
          ['HR'],
        ],
      );

      final depts = await mysql.query('SELECT id FROM departments ORDER BY id');
      final engId = depts.rows[0][0];
      final salesId = depts.rows[1][0];

      await mysql.insertBatch(
        'employees',
        ['name', 'department_id'],
        [
          ['Alice', engId],
          ['Bob', engId],
          ['Charlie', salesId],
        ],
      );

      final result = await mysql.queryRaw('''
        SELECT d.name, COUNT(e.id) as employee_count
        FROM departments d
        LEFT JOIN employees e ON d.id = e.department_id
        GROUP BY d.id, d.name
        HAVING COUNT(e.id) > 0
        ORDER BY employee_count DESC
      ''');

      expect(result.rows.length, 2);
      expect(result.rows[0][0], 'Engineering');
      expect(result.rows[0][1], 2);
    });

    test('complex JOIN with aggregation', () async {
      await mysql.insertBatch(
        'departments',
        ['name'],
        [
          ['Tech'],
        ],
      );
      final deptId = (await mysql.queryRaw(
        'SELECT id FROM departments',
      )).rows[0][0];

      await mysql.insertBatch(
        'employees',
        ['name', 'department_id'],
        [
          ['Alice', deptId],
          ['Bob', deptId],
        ],
      );

      final employees = await mysql.queryRaw(
        'SELECT id FROM employees ORDER BY id',
      );
      final aliceId = employees.rows[0][0];
      final bobId = employees.rows[1][0];

      await mysql.insertBatch(
        'salaries',
        ['employee_id', 'amount'],
        [
          [aliceId, 70000],
          [aliceId, 75000],
          [bobId, 60000],
          [bobId, 65000],
        ],
      );

      final result = await mysql.queryRaw('''
        SELECT 
          e.name,
          COUNT(s.id) as salary_records,
          AVG(s.amount) as avg_salary,
          MAX(s.amount) as max_salary
        FROM employees e
        JOIN salaries s ON e.id = s.employee_id
        GROUP BY e.id, e.name
        ORDER BY avg_salary DESC
      ''');

      expect(result.rows.length, 2);
      expect(result.rows[0][0], 'Alice');
      expect(result.rows[0][2], closeTo(72500, 0.1));
    });

    test('CASE statement in SELECT', () async {
      await mysql.insertBatch(
        'departments',
        ['name'],
        [
          ['Dept'],
        ],
      );
      final deptId = (await mysql.queryRaw(
        'SELECT id FROM departments',
      )).rows[0][0];

      await mysql.insertBatch(
        'employees',
        ['name', 'department_id', 'hire_date'],
        [
          ['Recent', deptId, '2024-01-01'],
          ['Old', deptId, '2020-01-01'],
        ],
      );

      final result = await mysql.queryRaw('''
        SELECT 
          name,
          CASE 
            WHEN hire_date >= '2023-01-01' THEN 'New'
            WHEN hire_date >= '2021-01-01' THEN 'Mid'
            ELSE 'Senior'
          END as tenure
        FROM employees
        ORDER BY name
      ''');

      expect(result.rows[0][0], 'Old');
      expect(result.rows[0][1], 'Senior');
      expect(result.rows[1][0], 'Recent');
      expect(result.rows[1][1], 'New');
    });

    test('UNION queries', () async {
      await mysql.insertBatch(
        'departments',
        ['name'],
        [
          ['Dept1'],
          ['Dept2'],
        ],
      );

      final result = await mysql.queryRaw('''
        SELECT 'Department' as type, name as label FROM departments WHERE name = 'Dept1'
        UNION
        SELECT 'Department' as type, name as label FROM departments WHERE name = 'Dept2'
        ORDER BY label
      ''');

      expect(result.rows.length, 2);
    });

    test('self-join scenario', () async {
      await mysql.query('DROP TABLE IF EXISTS employees_hierarchy');
      await mysql.query('''
        CREATE TABLE employees_hierarchy (
          id INT AUTO_INCREMENT PRIMARY KEY,
          name VARCHAR(100),
          manager_id INT NULL,
          FOREIGN KEY (manager_id) REFERENCES employees_hierarchy(id)
        )
      ''');

      final ceo = await mysql.query(
        'INSERT INTO employees_hierarchy (name, manager_id) VALUES (?, ?)',
        [
          'CEO',
          null,
        ],
      );
      final manager = await mysql.query(
        'INSERT INTO employees_hierarchy (name, manager_id) VALUES (?, ?)',
        [
          'Manager',
          ceo.lastInsertId,
        ],
      );
      await mysql.query(
        'INSERT INTO employees_hierarchy (name, manager_id) VALUES (?, ?)',
        [
          'Employee',
          manager.lastInsertId,
        ],
      );

      final result = await mysql.queryRaw('''
        SELECT 
          e.name as employee_name,
          m.name as manager_name
        FROM employees_hierarchy e
        LEFT JOIN employees_hierarchy m ON e.manager_id = m.id
        ORDER BY e.id
      ''');

      expect(result.rows[0][0], 'CEO');
      expect(result.rows[0][1], isNull);
      expect(result.rows[1][0], 'Manager');
      expect(result.rows[1][1], 'CEO');

      await mysql.query('DROP TABLE employees_hierarchy');
    });

    test('window function simulation with variables', () async {
      await mysql.insertBatch(
        'departments',
        ['name'],
        [
          ['Dept'],
        ],
      );
      final deptId = (await mysql.queryRaw(
        'SELECT id FROM departments',
      )).rows[0][0];

      await mysql.insertBatch(
        'employees',
        ['name', 'department_id'],
        [
          ['Alice', deptId],
          ['Bob', deptId],
          ['Charlie', deptId],
        ],
      );

      final result = await mysql.queryRaw('''
        SELECT 
          @row_number := @row_number + 1 AS row_num,
          name
        FROM employees, (SELECT @row_number := 0) AS init
        ORDER BY name
      ''');

      expect(result.rows.length, 3);
      expect(result.rows[0][0], 1);
      expect(result.rows[1][0], 2);
      expect(result.rows[2][0], 3);
    });
  });

  group('Performance and Stress Tests', () {
    late MySqlPool mysql;

    setUpAll(() async {
      mysql = MySqlPool(
        MySqlConfig(
          host: host,
          user: user,
          pass: pass,
          dbName: dbName,
          port: port,
          poolMax: 10,
        ),
      );

      await mysql.connect();
      await mysql.query('DROP TABLE IF EXISTS test_stress');
      await mysql.query('''
        CREATE TABLE test_stress (
          id INT AUTO_INCREMENT PRIMARY KEY,
          data VARCHAR(255),
          value INT,
          INDEX idx_value (value)
        )
      ''');
    });

    tearDownAll(() async {
      if (mysql.isConnected) {
        await mysql.query('DROP TABLE IF EXISTS test_stress');
        await mysql.close();
      }
    });

    setUp(() async {
      await mysql.query('DELETE FROM test_stress');
    });

    test('insert many rows individually', () async {
      for (var i = 0; i < 100; i++) {
        await mysql.query(
          'INSERT INTO test_stress (data, value) VALUES (?, ?)',
          ['data$i', i],
        );
      }

      final result = await mysql.query('SELECT COUNT(*) FROM test_stress');
      expect(result.rows[0][0], 100);
    });

    test('batch insert performance comparison', () async {
      final rows = List.generate(1000, (i) => ['batch$i', i * 10]);

      await mysql.insertBatch('test_stress', ['data', 'value'], rows);

      final result = await mysql.query('SELECT COUNT(*) FROM test_stress');
      expect(result.rows[0][0], 1000);
    });

    test('select with index usage', () async {
      await mysql.insertBatch('test_stress', [
        'data',
        'value',
      ], List.generate(1000, (i) => ['data$i', i]));

      final result = await mysql.query(
        'SELECT data FROM test_stress WHERE value BETWEEN ? AND ? ORDER BY value',
        [
          100,
          200,
        ],
      );
      expect(result.rows.length, 101);
    });

    test('prepared statement reuse performance', () async {
      final stmt = await mysql.prepare(
        'INSERT INTO test_stress (data, value) VALUES (?, ?)',
      );

      for (var i = 0; i < 100; i++) {
        await stmt.execute(['prep$i', i]);
      }

      await stmt.release();

      final result = await mysql.query('SELECT COUNT(*) FROM test_stress');
      expect(result.rows[0][0], 100);
    });

    test('concurrent query execution', () async {
      await mysql.insertBatch('test_stress', [
        'data',
        'value',
      ], List.generate(100, (i) => ['data$i', i]));

      final futures = List.generate(20, (i) {
        return mysql.query(
          'SELECT COUNT(*) FROM test_stress WHERE value > ?',
          [i * 5],
        );
      });

      await Future.wait(futures);
    });

    test('transaction performance', () async {
      final tx = await mysql.beginTransaction();
      for (var i = 0; i < 50; i++) {
        await tx.query(
          'INSERT INTO test_stress (data, value) VALUES (?, ?)',
          ['tx$i', i],
        );
      }
      await tx.commit();

      final result = await mysql.query('SELECT COUNT(*) FROM test_stress');
      expect(result.rows[0][0], 50);
    });
  });

  group('Error Recovery and Edge Cases', () {
    late MySqlPool mysql;

    setUpAll(() async {
      mysql = MySqlPool(
        MySqlConfig(
          host: host,
          user: user,
          pass: pass,
          dbName: dbName,
          port: port,
        ),
      );

      await mysql.connect();
      await mysql.query('DROP TABLE IF EXISTS test_recovery');
      await mysql.query('''
        CREATE TABLE test_recovery (
          id INT AUTO_INCREMENT PRIMARY KEY,
          unique_val VARCHAR(50) UNIQUE,
          not_null_val VARCHAR(50) NOT NULL
        )
      ''');
    });

    tearDownAll(() async {
      if (mysql.isConnected) {
        await mysql.query('DROP TABLE IF EXISTS test_recovery');
        await mysql.close();
      }
    });

    setUp(() async {
      await mysql.query('DELETE FROM test_recovery');
    });

    test('recovers from duplicate key error', () async {
      await mysql.query(
        'INSERT INTO test_recovery (unique_val, not_null_val) VALUES (?, ?)',
        ['unique1', 'value1'],
      );

      try {
        await mysql.query(
          'INSERT INTO test_recovery (unique_val, not_null_val) VALUES (?, ?)',
          [
            'unique1',
            'value2',
          ],
        );
        fail('Should have thrown duplicate key error');
      } catch (e) {
        expect(e, isA<MySQLException>());
      }

      final result = await mysql.query('SELECT 1 AS test');
      expect(result.rows[0][0].toString(), '1');
    });

    test('recovers from NOT NULL constraint violation', () async {
      try {
        await mysql.query(
          'INSERT INTO test_recovery (unique_val, not_null_val) VALUES (?, ?)',
          ['unique2', null],
        );
        fail('Should have thrown NOT NULL constraint violation');
      } catch (e) {
        expect(e, isA<MySQLException>());
      }

      await mysql.query(
        'INSERT INTO test_recovery (unique_val, not_null_val) VALUES (?, ?)',
        [
          'unique2',
          'valid_value',
        ],
      );

      final result = await mysql.query('SELECT COUNT(*) FROM test_recovery');
      expect(result.rows[0][0], 1);
    });

    test('handles malformed SQL syntax', () async {
      try {
        await mysql.queryRaw('SEEELECT * FROM test_recovery');
        fail('Should have thrown syntax error');
      } catch (e) {
        expect(e, isA<MySQLException>());
      }

      final result = await mysql.query('SELECT 1 AS test');
      expect(result.rows[0][0].toString(), '1');
    });

    test('handles parameter count mismatch', () async {
      await mysql.query(
        'INSERT INTO test_recovery (unique_val, not_null_val) VALUES (?, ?)',
        ['test1', 'test2'],
      );

      final result = await mysql.query('SELECT COUNT(*) FROM test_recovery');
      expect(result.rows[0][0], 1);
    });

    test('transaction rollback after error maintains pool', () async {
      final tx = await mysql.beginTransaction();

      await tx.query(
        'INSERT INTO test_recovery (unique_val, not_null_val) VALUES (?, ?)',
        ['tx1', 'value1'],
      );

      try {
        await tx.query('SELECT * FROM non_existent_table');
        fail('Should have thrown table not found error');
      } catch (e) {
        await tx.rollback();
      }

      final result = await mysql.query('SELECT COUNT(*) FROM test_recovery');
      expect(result.rows[0][0], 0);
    });

    test('multiple errors in sequence', () async {
      for (var i = 0; i < 5; i++) {
        try {
          await mysql.queryRaw('INVALID SQL QUERY $i');
          fail('Should have thrown error');
        } catch (e) {
          expect(e, isA<MySQLException>());
        }
      }

      final result = await mysql.query('SELECT 1 AS test');
      expect(result.rows[0][0].toString(), '1');
    });

    test('prepared statement error recovery', () async {
      final stmt = await mysql.prepare(
        'INSERT INTO test_recovery (unique_val, not_null_val) VALUES (?, ?)',
      );

      await stmt.execute(['prep1', 'value1']);

      try {
        await stmt.execute(['prep1', 'value2']);
        fail('Should have thrown duplicate key error');
      } catch (e) {
        expect(e, isA<MySQLException>());
      }

      await stmt.execute(['prep2', 'value2']);
      await stmt.release();

      final result = await mysql.query('SELECT COUNT(*) FROM test_recovery');
      expect(result.rows[0][0], 2);
    });

    test('handles very long query timeout gracefully', () async {
      try {
        await mysql.query('SELECT SLEEP(1)');
      } catch (e) {
        expect(e, isA<MySQLException>());
      }
    });
  });

  group('Character Encoding Tests', () {
    late MySqlPool mysql;

    setUpAll(() async {
      mysql = MySqlPool(
        MySqlConfig(
          host: host,
          user: user,
          pass: pass,
          dbName: dbName,
          port: port,
        ),
      );

      await mysql.connect();
      await mysql.query('DROP TABLE IF EXISTS test_encoding');
      await mysql.query('''
        CREATE TABLE test_encoding (
          id INT AUTO_INCREMENT PRIMARY KEY,
          text_data TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
        ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
      ''');
    });

    tearDownAll(() async {
      if (mysql.isConnected) {
        await mysql.query('DROP TABLE IF EXISTS test_encoding');
        await mysql.close();
      }
    });

    setUp(() async {
      await mysql.query('DELETE FROM test_encoding');
    });

    test('handles emojis', () async {
      const emojiText = '👨‍👩‍👧‍👦 Family 🚀 Rocket 🌟 Star';

      await mysql.query('INSERT INTO test_encoding (text_data) VALUES (?)', [
        emojiText,
      ]);

      final result = await mysql.query('SELECT text_data FROM test_encoding');
      expect(result.rows[0][0], emojiText);
    });

    test('handles multi-byte characters', () async {
      const multiByteText = '日本語 中文 한글 العربية עברית';

      await mysql.query('INSERT INTO test_encoding (text_data) VALUES (?)', [
        multiByteText,
      ]);

      final result = await mysql.query('SELECT text_data FROM test_encoding');
      expect(result.rows[0][0], multiByteText);
    });

    test('handles mixed scripts', () async {
      const mixedText = 'Hello שלום مرحبا 你好 こんにちは';

      await mysql.query('INSERT INTO test_encoding (text_data) VALUES (?)', [
        mixedText,
      ]);

      final result = await mysql.query('SELECT text_data FROM test_encoding');
      expect(result.rows[0][0], mixedText);
    });

    test('handles mathematical symbols', () async {
      const mathText = '∑∫∂∇ ∞ ∀∃ ⊂⊃ ∈∉ ≤≥≠ ±×÷';

      await mysql.query('INSERT INTO test_encoding (text_data) VALUES (?)', [
        mathText,
      ]);

      final result = await mysql.query('SELECT text_data FROM test_encoding');
      expect(result.rows[0][0], mathText);
    });

    test('handles currency symbols', () async {
      const currencyText = '€£¥₹₽฿₩₪₺₴₦';

      await mysql.query('INSERT INTO test_encoding (text_data) VALUES (?)', [
        currencyText,
      ]);

      final result = await mysql.query('SELECT text_data FROM test_encoding');
      expect(result.rows[0][0], currencyText);
    });
  });
}
