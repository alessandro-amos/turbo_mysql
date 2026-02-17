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

  group('MySqlConfig Unit Tests', () {
    test('creates basic connection string', () {
      final config = MySqlConfig(
        host: host,
        user: user,
        pass: pass,
      );

      final connString = config.toConnectionString();
      expect(connString, contains('mysql://$user:$pass@$host:$port'));
      expect(connString, contains('pool_min=1'));
      expect(connString, contains('pool_max=10'));
    });

    test('handles empty database name', () {
      final config = MySqlConfig(
        host: host,
        user: user,
        pass: pass,
        dbName: '',
      );

      final connString = config.toConnectionString();
      expect(connString, isNot(contains('$host:$port?/')));
      expect(connString, contains('$host:$port?'));
    });

    test('encodes special characters in credentials', () {
      final config = MySqlConfig(
        host: host,
        user: 'user@domain.com',
        pass: 'p@ss!w#rd%',
      );

      final connString = config.toConnectionString();
      expect(connString, contains('user%40domain.com'));
      expect(connString, contains('p%40ss%21w%23rd%25'));
    });

    test('builds full connection string with all parameters', () {
      final config = MySqlConfig(
        host: '192.168.1.100',
        user: 'admin',
        pass: 'secure123',
        dbName: 'mydb',
        port: 3307,
        poolMin: 5,
        poolMax: 20,
        tcpNodelay: false,
        tcpKeepalive: 10000,
        connTtl: 60000,
        absConnTtl: 120000,
        absConnTtlJitter: 5000,
        stmtCacheSize: 100,
        requireSsl: true,
        verifyCa: true,
        verifyIdentity: false,
        preferSocket: true,
        socket: '/var/run/mysqld/mysqld.sock',
        compression: 'fast',
        maxAllowedPacket: 16777216,
        waitTimeout: 28800,
        secureAuth: false,
        clientFoundRows: true,
        enableCleartextPlugin: false,
        init: ['SET NAMES utf8mb4', 'SET time_zone = "+00:00"'],
        setup: ['SET SESSION sql_mode = "STRICT_ALL_TABLES"'],
      );

      final connString = config.toConnectionString();
      expect(connString, contains('192.168.1.100:3307'));
      expect(connString, contains('pool_min=5'));
      expect(connString, contains('pool_max=20'));
      expect(connString, contains('tcp_nodelay=false'));
      expect(connString, contains('tcp_keepalive=10000'));
      expect(connString, contains('conn_ttl=60000'));
      expect(connString, contains('abs_conn_ttl=120000'));
      expect(connString, contains('abs_conn_ttl_jitter=5000'));
      expect(connString, contains('stmt_cache_size=100'));
      expect(connString, contains('require_ssl=true'));
      expect(connString, contains('verify_ca=true'));
      expect(connString, contains('verify_identity=false'));
      expect(connString, contains('prefer_socket=true'));
      expect(connString, contains('compression=fast'));
      expect(connString, contains('max_allowed_packet=16777216'));
      expect(connString, contains('wait_timeout=28800'));
      expect(connString, contains('secure_auth=false'));
      expect(connString, contains('client_found_rows=true'));
      expect(connString, contains('enable_cleartext_plugin=false'));
      expect(connString, contains('init=SET%20NAMES%20utf8mb4'));
      expect(
        connString,
        contains(
          'setup=SET%20SESSION%20sql_mode%20%3D%20%22STRICT_ALL_TABLES%22',
        ),
      );
    });

    test('copyWith replaces only specified fields', () {
      final config1 = MySqlConfig(
        host: host,
        user: user,
        pass: pass,
        dbName: dbName,
        port: port,
        poolMax: 10,
      );

      final config2 = config1.copyWith(host: 'remote.server.com', poolMax: 50);

      expect(config2.host, 'remote.server.com');
      expect(config2.user, user);
      expect(config2.pass, pass);
      expect(config2.dbName, dbName);
      expect(config2.port, 3306);
      expect(config2.poolMax, 50);
      expect(config1.poolMax, 10);
    });

    test('copyWith preserves null optional parameters', () {
      final config1 = MySqlConfig(
        host: host,
        user: user,
        pass: pass,
      );

      final config2 = config1.copyWith(port: 3307);

      expect(config2.tcpKeepalive, isNull);
      expect(config2.requireSsl, isNull);
      expect(config2.socket, isNull);
    });

    test('handles multiple init and setup statements', () {
      final config = MySqlConfig(
        host: host,
        user: user,
        pass: pass,
        init: [
          'SET NAMES utf8mb4',
          'SET time_zone = "+00:00"',
          'SET sql_mode = "STRICT_ALL_TABLES"',
        ],
        setup: [
          'SET SESSION max_execution_time = 10000',
          'SET SESSION lock_wait_timeout = 5',
        ],
      );

      final connString = config.toConnectionString();
      expect(connString, contains('init=SET%20NAMES%20utf8mb4'));
      expect(
        connString,
        contains('init=SET%20time_zone%20%3D%20%22%2B00%3A00%22'),
      );
      expect(
        connString,
        contains('setup=SET%20SESSION%20max_execution_time%20%3D%2010000'),
      );
    });

    test('handles unusual but valid database names', () {
      final config = MySqlConfig(
        host: host,
        user: user,
        pass: pass,
        dbName: 'my-database_2024',
      );

      final connString = config.toConnectionString();
      expect(connString, contains('/my-database_2024'));
    });
  });

  group('QueryResult Unit Tests', () {
    test('creates result with data', () {
      final result = QueryResult(
        columns: ['id', 'name', 'age'],
        rows: [
          [1, 'Alice', 30],
          [2, 'Bob', 25],
        ],
        affectedRows: 2,
        lastInsertId: 0,
      );

      expect(result.columns.length, 3);
      expect(result.rows.length, 2);
      expect(result.affectedRows, 2);
    });

    test('converts rows to maps correctly', () {
      final result = QueryResult(
        columns: ['id', 'name', 'age'],
        rows: [
          [1, 'Alice', 30],
          [2, 'Bob', 25],
        ],
        affectedRows: 2,
        lastInsertId: 0,
      );

      final maps = result.asMaps;
      expect(maps.length, 2);
      expect(maps[0]['id'], 1);
      expect(maps[0]['name'], 'Alice');
      expect(maps[0]['age'], 30);
      expect(maps[1]['id'], 2);
      expect(maps[1]['name'], 'Bob');
    });

    test('handles empty result set', () {
      final result = QueryResult(
        columns: ['id', 'name'],
        rows: [],
        affectedRows: 0,
        lastInsertId: 0,
      );

      final maps = result.asMaps;
      expect(maps, isEmpty);
    });

    test('handles null values in rows', () {
      final result = QueryResult(
        columns: ['id', 'name', 'age'],
        rows: [
          [1, 'Alice', null],
          [2, null, 25],
        ],
        affectedRows: 2,
        lastInsertId: 0,
      );

      final maps = result.asMaps;
      expect(maps[0]['age'], isNull);
      expect(maps[1]['name'], isNull);
    });

    test('handles mismatched row and column lengths', () {
      final result = QueryResult(
        columns: ['id', 'name', 'age'],
        rows: [
          [1, 'Alice'],
          [2, 'Bob', 25, 'extra'],
        ],
        affectedRows: 2,
        lastInsertId: 0,
      );

      final maps = result.asMaps;
      expect(maps[0].containsKey('age'), isFalse);
      expect(maps[1]['age'], 25);
    });

    test('toString returns formatted string', () {
      final result = QueryResult(
        columns: ['id'],
        rows: [
          [1],
          [2],
          [3],
        ],
        affectedRows: 3,
        lastInsertId: 100,
      );

      final str = result.toString();
      expect(str, contains('rows: 3'));
      expect(str, contains('affected: 3'));
      expect(str, contains('id: 100'));
    });

    test('handles large lastInsertId', () {
      final result = QueryResult(
        columns: ['id'],
        rows: [],
        affectedRows: 1,
        lastInsertId: 9223372036854775807,
      );

      expect(result.lastInsertId, 9223372036854775807);
    });
  });

  group('MySQLException Unit Tests', () {
    test('creates exception with message', () {
      final exception = MySQLException('Connection failed');
      expect(exception.message, 'Connection failed');
    });

    test('toString formats correctly', () {
      final exception = MySQLException('Query error: syntax error near SELECT');
      expect(
        exception.toString(),
        'MySQLException: Query error: syntax error near SELECT',
      );
    });

    test('can be caught as Exception', () {
      expect(() => throw MySQLException('Test'), throwsA(isA<Exception>()));
    });

    test('preserves message in catch block', () {
      try {
        throw MySQLException('Custom error message');
      } catch (e) {
        expect(e, isA<MySQLException>());
        expect((e as MySQLException).message, 'Custom error message');
      }
    });
  });

  group('Integration Tests - Basic Operations', () {
    late MySqlPool mysql;

    setUpAll(() async {
      mysql = MySqlPool(
        MySqlConfig(
          host: '127.0.0.1',
          user: user,
          pass: pass,
          dbName: dbName,
          port: port,
          poolMax: 10,
        ),
      );

      await mysql.ensureDatabase();
      await mysql.connect();

      await mysql.query('DROP TABLE IF EXISTS test_users');
      await mysql.query('DROP TABLE IF EXISTS test_products');
      await mysql.query('DROP TABLE IF EXISTS test_orders');
      await mysql.query('DROP TABLE IF EXISTS test_logs');

      await mysql.query('''
        CREATE TABLE test_users (
          id INT AUTO_INCREMENT PRIMARY KEY,
          username VARCHAR(50) NOT NULL UNIQUE,
          email VARCHAR(100),
          age INT,
          balance DECIMAL(10,2),
          is_active BOOLEAN DEFAULT TRUE,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          metadata JSON,
          profile_pic BLOB
        )
      ''');

      await mysql.query('''
        CREATE TABLE test_products (
          id INT AUTO_INCREMENT PRIMARY KEY,
          name VARCHAR(255) NOT NULL,
          price DECIMAL(10,2),
          stock INT DEFAULT 0,
          INDEX idx_name (name)
        )
      ''');

      await mysql.query('''
        CREATE TABLE test_orders (
          id INT AUTO_INCREMENT PRIMARY KEY,
          user_id INT,
          product_id INT,
          quantity INT,
          order_date DATETIME,
          FOREIGN KEY (user_id) REFERENCES test_users(id) ON DELETE CASCADE,
          FOREIGN KEY (product_id) REFERENCES test_products(id) ON DELETE CASCADE
        )
      ''');

      await mysql.query('''
        CREATE TABLE test_logs (
          id BIGINT AUTO_INCREMENT PRIMARY KEY,
          level ENUM('DEBUG', 'INFO', 'WARNING', 'ERROR') DEFAULT 'INFO',
          message TEXT,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
      ''');
    });

    tearDownAll(() async {
      if (mysql.isConnected) {
        await mysql.query('DROP TABLE IF EXISTS test_orders');
        await mysql.query('DROP TABLE IF EXISTS test_products');
        await mysql.query('DROP TABLE IF EXISTS test_users');
        await mysql.query('DROP TABLE IF EXISTS test_logs');
        await mysql.close();
      }
    });

    setUp(() async {
      await mysql.query('DELETE FROM test_orders');
      await mysql.query('DELETE FROM test_products');
      await mysql.query('DELETE FROM test_users');
      await mysql.query('DELETE FROM test_logs');
    });

    test('SELECT 1 returns correct value', () async {
      final result = await mysql.query('SELECT 1 AS num, "test" AS str');
      expect(result.rows.length, 1);
      expect(result.rows[0][0].toString(), '1');
      expect(result.rows[0][1].toString(), 'test');
    });

    test('INSERT returns lastInsertId', () async {
      final result = await mysql.query(
        'INSERT INTO test_users (username, email) VALUES (?, ?)',
        [
          'john_doe',
          'john@example.com',
        ],
      );

      expect(result.affectedRows, greaterThan(0));
      expect(result.lastInsertId, greaterThan(0));

      final userId = result.lastInsertId;
      final selectResult = await mysql.query(
        'SELECT id, username FROM test_users WHERE id = ?',
        [userId],
      );

      expect(selectResult.rows[0][0], userId);
      expect(selectResult.rows[0][1], 'john_doe');
    });

    test('UPDATE affects correct rows', () async {
      await mysql.query(
        'INSERT INTO test_users (username, age) VALUES (?, ?)',
        ['user1', 25],
      );
      await mysql.query(
        'INSERT INTO test_users (username, age) VALUES (?, ?)',
        ['user2', 30],
      );

      final updateResult = await mysql.query(
        'UPDATE test_users SET age = ? WHERE age > ?',
        [35, 26],
      );

      expect(updateResult.affectedRows, 1);

      final result = await mysql.query(
        'SELECT username, age FROM test_users ORDER BY age',
      );
      expect(result.rows[0][1], 25);
      expect(result.rows[1][1], 35);
    });

    test('DELETE removes correct rows', () async {
      await mysql.query('INSERT INTO test_users (username) VALUES (?)', [
        'temp_user1',
      ]);
      await mysql.query('INSERT INTO test_users (username) VALUES (?)', [
        'temp_user2',
      ]);
      await mysql.query('INSERT INTO test_users (username) VALUES (?)', [
        'keep_user',
      ]);

      final deleteResult = await mysql.query(
        'DELETE FROM test_users WHERE username LIKE ?',
        ['temp_%'],
      );

      expect(deleteResult.affectedRows, 2);

      final result = await mysql.query('SELECT COUNT(*) FROM test_users');
      expect(result.rows[0][0], 1);
    });

    test('SELECT with multiple WHERE conditions', () async {
      await mysql.insertBatch(
        'test_users',
        ['username', 'age', 'is_active'],
        [
          ['user1', 25, true],
          ['user2', 30, true],
          ['user3', 35, false],
          ['user4', 40, true],
        ],
      );

      final result = await mysql.query(
        'SELECT username FROM test_users WHERE age > ? AND is_active = ? ORDER BY age',
        [28, true],
      );

      expect(result.rows.length, 2);
      expect(result.rows[0][0], 'user2');
      expect(result.rows[1][0], 'user4');
    });

    test('SELECT with IN clause', () async {
      await mysql.insertBatch(
        'test_users',
        ['username', 'age'],
        [
          ['user1', 20],
          ['user2', 25],
          ['user3', 30],
          ['user4', 35],
          ['user5', 40],
        ],
      );

      final result = await mysql.query(
        'SELECT username FROM test_users WHERE age IN (?, ?, ?) ORDER BY age',
        [
          25,
          35,
          40,
        ],
      );

      expect(result.rows.length, 3);
      expect(result.rows.map((r) => r[0]).toList(), [
        'user2',
        'user4',
        'user5',
      ]);
    });

    test('SELECT with LIMIT and OFFSET', () async {
      for (var i = 1; i <= 10; i++) {
        await mysql.query(
          'INSERT INTO test_users (username, age) VALUES (?, ?)',
          ['user$i', 20 + i],
        );
      }

      final page2 = await mysql.query(
        'SELECT username FROM test_users ORDER BY age LIMIT ? OFFSET ?',
        [3, 3],
      );

      expect(page2.rows.length, 3);
      expect(page2.rows[0][0], 'user4');
      expect(page2.rows[2][0], 'user6');
    });

    test('SELECT with aggregation functions', () async {
      await mysql.insertBatch(
        'test_users',
        ['username', 'age', 'balance'],
        [
          ['user1', 25, 100.50],
          ['user2', 30, 250.75],
          ['user3', 35, 175.25],
        ],
      );

      final result = await mysql.queryRaw('''
        SELECT 
          COUNT(*) as total,
          MIN(age) as min_age,
          MAX(age) as max_age,
          AVG(age) as avg_age,
          SUM(balance) as total_balance
        FROM test_users
      ''');

      final row = result.rows[0];
      expect(row[0], 3);
      expect(row[1], 25);
      expect(row[2], 35);
      expect(row[3].round(), 30);
      expect(row[4], closeTo(526.50, 0.01));
    });

    test('SELECT with JOIN operations', () async {
      final user1 = await mysql.query(
        'INSERT INTO test_users (username) VALUES (?)',
        ['buyer1'],
      );
      final user2 = await mysql.query(
        'INSERT INTO test_users (username) VALUES (?)',
        ['buyer2'],
      );

      final product1 = await mysql.query(
        'INSERT INTO test_products (name, price) VALUES (?, ?)',
        [
          'Laptop',
          1200.00,
        ],
      );
      final product2 = await mysql.query(
        'INSERT INTO test_products (name, price) VALUES (?, ?)',
        ['Mouse', 25.50],
      );

      await mysql.query(
        'INSERT INTO test_orders (user_id, product_id, quantity) VALUES (?, ?, ?)',
        [
          user1.lastInsertId,
          product1.lastInsertId,
          1,
        ],
      );
      await mysql.query(
        'INSERT INTO test_orders (user_id, product_id, quantity) VALUES (?, ?, ?)',
        [
          user2.lastInsertId,
          product2.lastInsertId,
          2,
        ],
      );

      final result = await mysql.queryRaw('''
        SELECT u.username, p.name, o.quantity
        FROM test_orders o
        JOIN test_users u ON o.user_id = u.id
        JOIN test_products p ON o.product_id = p.id
        ORDER BY u.username
      ''');

      expect(result.rows.length, 2);
      expect(result.rows[0][0], 'buyer1');
      expect(result.rows[0][1], 'Laptop');
      expect(result.rows[1][0], 'buyer2');
    });

    test('SELECT with GROUP BY and HAVING', () async {
      await mysql.insertBatch(
        'test_users',
        ['username', 'age'],
        [
          ['user1', 25],
          ['user2', 25],
          ['user3', 30],
          ['user4', 30],
          ['user5', 30],
          ['user6', 35],
        ],
      );

      final result = await mysql.queryRaw('''
        SELECT age, COUNT(*) as count
        FROM test_users
        GROUP BY age
        HAVING COUNT(*) >= 2
        ORDER BY age
      ''');

      expect(result.rows.length, 2);
      expect(result.rows[0][0].toString(), '25');
      expect(result.rows[0][1].toString(), '2');
      expect(result.rows[1][0].toString(), '30');
      expect(result.rows[1][1].toString(), '3');
    });

    test('handles DECIMAL values correctly', () async {
      await mysql.query(
        'INSERT INTO test_users (username, balance) VALUES (?, ?)',
        ['decimal_user', 12345.67],
      );

      final result = await mysql.queryRaw(
        'SELECT balance FROM test_users WHERE username = "decimal_user"',
      );

      final balance = result.rows[0][0];
      expect(balance, closeTo(12345.67, 0.01));
    });

    test('handles very large numbers', () async {
      await mysql.query('INSERT INTO test_logs (message) VALUES (?)', [
        'Test log',
      ]);

      final result = await mysql.query(
        'SELECT id FROM test_logs ORDER BY id DESC LIMIT 1',
      );
      expect(result.rows[0][0], isA<int>());
    });

    test('handles empty strings vs NULL', () async {
      await mysql.query(
        'INSERT INTO test_users (username, email) VALUES (?, ?)',
        ['user_empty', ''],
      );
      await mysql.query(
        'INSERT INTO test_users (username, email) VALUES (?, ?)',
        ['user_null', null],
      );

      final result = await mysql.query(
        'SELECT username, email FROM test_users WHERE username IN ("user_empty", "user_null") ORDER BY username',
      );

      expect(result.rows[0][1], '');
      expect(result.rows[1][1], isNull);
    });

    test('handles DateTime parameters', () async {
      final now = DateTime.now().copyWith(millisecond: 0, microsecond: 0);

      await mysql.query(
        'INSERT INTO test_users (username, created_at) VALUES (?, ?)',
        ['datetime_user', DateTime.now()],
      );

      final result = await mysql.query(
        'SELECT created_at FROM test_users WHERE username = ?',
        ['datetime_user'],
      );

      expect(
        result.rows[0][0],
        isA<DateTime>(),
      );

      expect(result.rows[0][0], equals(now));
    });

    test('handles BLOB data correctly', () async {
      final imageData = Uint8List.fromList(List.generate(1000, (i) => i % 256));

      await mysql.query(
        'INSERT INTO test_users (username, profile_pic) VALUES (?, ?)',
        ['blob_user', imageData],
      );

      final result = await mysql.query(
        'SELECT profile_pic FROM test_users WHERE username = ?',
        ['blob_user'],
      );

      final retrieved = result.rows[0][0] as List<int>;
      expect(retrieved.length, 1000);
      expect(retrieved[0], 0);
      expect(retrieved[999], 999 % 256);
    });

    test('handles multiple parameter types in single query', () async {
      final result = await mysql.query(
        'SELECT ? as int_val, ? as float_val, ? as str_val, ? as bool_val, ? as null_val',
        [42, 3.14159, 'Hello World', true, null],
      );

      final row = result.rows[0];
      expect(row[0], 42);
      expect(row[1], closeTo(3.14159, 0.00001));
      expect(row[2], 'Hello World');
      expect(row[3], 1);
      expect(row[4], isNull);
    });

    test('handles special characters in strings', () async {
      const specialText =
          'Test with "quotes", \'apostrophes\', and \backslashes\\';

      await mysql.query(
        'INSERT INTO test_users (username, email) VALUES (?, ?)',
        ['special_user', specialText],
      );

      final result = await mysql.query(
        'SELECT email FROM test_users WHERE username = ?',
        ['special_user'],
      );

      expect(result.rows[0][0], specialText);
    });

    test('handles Unicode characters', () async {
      const unicodeText = '你好世界 🚀 émojis çédille';

      await mysql.query(
        'INSERT INTO test_users (username, email) VALUES (?, ?)',
        ['unicode_user', unicodeText],
      );

      final result = await mysql.query(
        'SELECT email FROM test_users WHERE username = ?',
        ['unicode_user'],
      );

      expect(result.rows[0][0], unicodeText);
    });

    test('throws MySQLException on syntax error', () async {
      expect(
        () async => await mysql.queryRaw('SLECT * FROM test_users'),
        throwsA(isA<MySQLException>()),
      );
    });

    test('throws MySQLException on non-existent table', () async {
      expect(
        () async => await mysql.query('SELECT * FROM non_existent_table'),
        throwsA(isA<MySQLException>()),
      );
    });

    test('throws MySQLException on constraint violation', () async {
      await mysql.query('INSERT INTO test_users (username) VALUES (?)', [
        'unique_user',
      ]);

      expect(
        () async => await mysql.query(
          'INSERT INTO test_users (username) VALUES (?)',
          ['unique_user'],
        ),
        throwsA(isA<MySQLException>()),
      );
    });

    test('throws MySQLException on foreign key violation', () async {
      expect(
        () async => await mysql.query(
          'INSERT INTO test_orders (user_id, product_id, quantity) VALUES (?, ?, ?)',
          [
            99999,
            99999,
            1,
          ],
        ),
        throwsA(isA<MySQLException>()),
      );
    });
  });

  group('Integration Tests - Batch Operations', () {
    late MySqlPool mysql;

    setUpAll(() async {
      mysql = MySqlPool(
        MySqlConfig(
          host: '127.0.0.1',
          user: user,
          pass: pass,
          dbName: dbName,
          port: port,
        ),
      );

      await mysql.connect();
      await mysql.query('DROP TABLE IF EXISTS test_batch');
      await mysql.query('''
        CREATE TABLE test_batch (
          id INT AUTO_INCREMENT PRIMARY KEY,
          code VARCHAR(50) UNIQUE,
          value INT,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        )
      ''');
    });

    tearDownAll(() async {
      if (mysql.isConnected) {
        await mysql.query('DROP TABLE IF EXISTS test_batch');
        await mysql.close();
      }
    });

    setUp(() async {
      await mysql.query('DELETE FROM test_batch');
    });

    test('insertBatch with empty rows returns 0', () async {
      final count = await mysql.insertBatch('test_batch', [
        'code',
        'value',
      ], []);
      expect(count, 0);
    });

    test('insertBatch with single row', () async {
      final count = await mysql.insertBatch(
        'test_batch',
        ['code', 'value'],
        [
          ['CODE1', 100],
        ],
      );

      expect(count, 1);

      final result = await mysql.query('SELECT code, value FROM test_batch');
      expect(result.rows.length, 1);
      expect(result.rows[0][0], 'CODE1');
    });

    test('insertBatch with multiple rows', () async {
      final count = await mysql.insertBatch(
        'test_batch',
        ['code', 'value'],
        [
          ['CODE1', 100],
          ['CODE2', 200],
          ['CODE3', 300],
          ['CODE4', 400],
          ['CODE5', 500],
        ],
      );

      expect(count, 5);

      final result = await mysql.query('SELECT COUNT(*) FROM test_batch');
      expect(result.rows[0][0], 5);
    });

    test('insertBatch with mixed data types', () async {
      await mysql.insertBatch(
        'test_batch',
        ['code', 'value'],
        [
          ['STR1', 10],
          ['STR2', 20],
          ['STR3', null],
        ],
      );

      final result = await mysql.queryRaw(
        'SELECT code, value FROM test_batch ORDER BY code',
      );

      expect(result.rows.length, 3);
      expect(result.rows[2][1], isNull);
    });

    test('insertBatch throws on column mismatch', () async {
      try {
        await mysql.insertBatch(
          'test_batch',
          ['code', 'value'],
          [
            ['CODE1', 100, 'extra'],
          ],
        );
      } catch (e) {
        expect(e, isA<MySQLException>());
      }
    });

    test('insertBatch throws on empty columns', () async {
      expect(
        () async => await mysql.insertBatch('test_batch', [], [
          [100],
        ]),
        throwsA(isA<MySQLException>()),
      );
    });

    test('upsertBatch updates existing row', () async {
      await mysql.query(
        'INSERT INTO test_batch (code, value) VALUES (?, ?)',
        ['DUP1', 100],
      );

      await mysql.upsertBatch(
        'test_batch',
        ['code', 'value'],
        [
          ['DUP1', 999],
          ['DUP2', 200],
        ],
      );

      final result = await mysql.queryRaw(
        'SELECT code, value FROM test_batch ORDER BY code',
      );

      expect(result.rows.length, 2);
      expect(result.rows[0][0], 'DUP1');
      expect(result.rows[0][1].toString(), '999');
      expect(result.rows[1][0], 'DUP2');
    });

    test('upsertBatch handles multiple duplicates', () async {
      await mysql.insertBatch(
        'test_batch',
        ['code', 'value'],
        [
          ['A', 1],
          ['B', 2],
          ['C', 3],
        ],
      );

      await mysql.upsertBatch(
        'test_batch',
        ['code', 'value'],
        [
          ['A', 10],
          ['B', 20],
          ['C', 30],
          ['D', 4],
        ],
      );

      final result = await mysql.queryRaw(
        'SELECT code, value FROM test_batch ORDER BY code',
      );

      expect(result.rows.length, 4);
      expect(result.rows[0][1].toString(), '10');
      expect(result.rows[1][1].toString(), '20');
      expect(result.rows[2][1].toString(), '30');
      expect(result.rows[3][1].toString(), '4');
    });

    test('insertBatch handles large batches', () async {
      final rows = List.generate(1000, (i) => ['CODE$i', i * 10]);

      final count = await mysql.insertBatch('test_batch', [
        'code',
        'value',
      ], rows);
      expect(count, 1000);

      final result = await mysql.query('SELECT COUNT(*) FROM test_batch');
      expect(result.rows[0][0], 1000);
    });
  });

  group('Integration Tests - Prepared Statements', () {
    late MySqlPool mysql;

    setUpAll(() async {
      mysql = MySqlPool(
        MySqlConfig(
          host: '127.0.0.1',
          user: user,
          pass: pass,
          dbName: dbName,
          port: port,
        ),
      );

      await mysql.connect();
      await mysql.query('DROP TABLE IF EXISTS test_prepared');
      await mysql.query('''
        CREATE TABLE test_prepared (
          id INT AUTO_INCREMENT PRIMARY KEY,
          name VARCHAR(100),
          value INT
        )
      ''');
    });

    tearDownAll(() async {
      if (mysql.isConnected) {
        await mysql.query('DROP TABLE IF EXISTS test_prepared');
        await mysql.close();
      }
    });

    setUp(() async {
      await mysql.query('DELETE FROM test_prepared');
    });

    test('prepare and execute statement once', () async {
      final stmt = await mysql.prepare(
        'INSERT INTO test_prepared (name, value) VALUES (?, ?)',
      );

      final result = await stmt.execute(['test1', 100]);
      expect(result.affectedRows, 1);

      await stmt.release();

      final selectResult = await mysql.queryRaw(
        'SELECT name, value FROM test_prepared',
      );
      expect(selectResult.rows[0][0], 'test1');
    });

    test('prepare and execute statement multiple times', () async {
      final stmt = await mysql.prepare(
        'INSERT INTO test_prepared (name, value) VALUES (?, ?)',
      );

      await stmt.execute(['item1', 10]);
      await stmt.execute(['item2', 20]);
      await stmt.execute(['item3', 30]);
      await stmt.execute(['item4', 40]);

      await stmt.release();

      final result = await mysql.query('SELECT COUNT(*) FROM test_prepared');
      expect(result.rows[0][0], 4);
    });

    test('prepare SELECT statement', () async {
      await mysql.insertBatch(
        'test_prepared',
        ['name', 'value'],
        [
          ['apple', 5],
          ['banana', 3],
          ['cherry', 8],
        ],
      );

      final stmt = await mysql.prepare(
        'SELECT name, value FROM test_prepared WHERE value > ? ORDER BY value',
      );

      final result1 = await stmt.execute([4]);
      expect(result1.rows.length, 2);
      expect(result1.rows[0][0], 'apple');

      final result2 = await stmt.execute([7]);
      expect(result2.rows.length, 1);
      expect(result2.rows[0][0], 'cherry');

      await stmt.release();
    });

    test('prepare UPDATE statement', () async {
      await mysql.insertBatch(
        'test_prepared',
        ['name', 'value'],
        [
          ['item1', 10],
          ['item2', 20],
        ],
      );

      final stmt = await mysql.prepare(
        'UPDATE test_prepared SET value = ? WHERE name = ?',
      );

      await stmt.execute([100, 'item1']);
      await stmt.execute([200, 'item2']);

      await stmt.release();

      final result = await mysql.queryRaw(
        'SELECT name, value FROM test_prepared ORDER BY name',
      );

      expect(result.rows[0][1].toString(), '100');
      expect(result.rows[1][1].toString(), '200');
    });

    test('prepare DELETE statement', () async {
      await mysql.insertBatch(
        'test_prepared',
        ['name', 'value'],
        [
          ['keep1', 10],
          ['delete1', 20],
          ['keep2', 30],
          ['delete2', 40],
        ],
      );

      final stmt = await mysql.prepare(
        'DELETE FROM test_prepared WHERE name LIKE ?',
      );

      await stmt.execute(['delete%']);
      await stmt.release();

      final result = await mysql.queryRaw(
        'SELECT name FROM test_prepared ORDER BY name',
      );
      expect(result.rows.length, 2);
      expect(result.rows[0][0], 'keep1');
      expect(result.rows[1][0], 'keep2');
    });

    test('throws error after statement is released', () async {
      final stmt = await mysql.prepare('SELECT 1');
      await stmt.release();

      expect(() async => await stmt.execute(), throwsA(isA<MySQLException>()));
    });

    test('multiple concurrent prepared statements', () async {
      final stmt1 = await mysql.prepare(
        'INSERT INTO test_prepared (name, value) VALUES (?, ?)',
      );
      final stmt2 = await mysql.prepare(
        'SELECT COUNT(*) FROM test_prepared WHERE value > ?',
      );

      await stmt1.execute(['a', 10]);
      await stmt1.execute(['b', 20]);

      final count1 = await stmt2.execute([5]);
      expect(count1.rows[0][0], 2);

      await stmt1.execute(['c', 30]);

      final count2 = await stmt2.execute([15]);
      expect(count2.rows[0][0], 2);

      await stmt1.release();
      await stmt2.release();
    });

    test('prepared statement with null parameters', () async {
      final stmt = await mysql.prepare(
        'INSERT INTO test_prepared (name, value) VALUES (?, ?)',
      );

      await stmt.execute(['null_value', null]);
      await stmt.execute([null, 100]);

      await stmt.release();

      final result = await mysql.queryRaw(
        'SELECT name, value FROM test_prepared ORDER BY id',
      );
      expect(result.rows[0][0], 'null_value');
      expect(result.rows[0][1], isNull);
      expect(result.rows[1][0], isNull);
      expect(result.rows[1][1], 100);
    });

    test('throws when preparing invalid SQL', () async {
      expect(
        () async => await mysql.prepare('SELECT * FRM invalid'),
        throwsA(isA<MySQLException>()),
      );
    });
  });

  group('Integration Tests - Transactions', () {
    late MySqlPool mysql;

    setUpAll(() async {
      mysql = MySqlPool(
        MySqlConfig(
          host: '127.0.0.1',
          user: user,
          pass: pass,
          dbName: dbName,
          port: port,
          poolMax: 10,
        ),
      );

      await mysql.connect();
      await mysql.query('DROP TABLE IF EXISTS test_accounts');
      await mysql.query('''
        CREATE TABLE test_accounts (
          id INT AUTO_INCREMENT PRIMARY KEY,
          name VARCHAR(100),
          balance DECIMAL(10,2) DEFAULT 0
        )
      ''');
    });

    tearDownAll(() async {
      if (mysql.isConnected) {
        await mysql.query('DROP TABLE IF EXISTS test_accounts');
        await mysql.close();
      }
    });

    setUp(() async {
      await mysql.query('DELETE FROM test_accounts');
    });

    test('transaction commit persists changes', () async {
      final tx = await mysql.beginTransaction();

      await tx.query(
        'INSERT INTO test_accounts (name, balance) VALUES (?, ?)',
        ['Alice', 1000.00],
      );

      final txResult = await tx.queryRaw(
        'SELECT balance FROM test_accounts WHERE name = "Alice"',
      );
      expect(txResult.rows[0][0], 1000.00);

      await tx.commit();

      final poolResult = await mysql.queryRaw(
        'SELECT balance FROM test_accounts WHERE name = "Alice"',
      );
      expect(poolResult.rows[0][0], 1000.00);
    });

    test('transaction rollback discards changes', () async {
      final tx = await mysql.beginTransaction();

      await tx.query(
        'INSERT INTO test_accounts (name, balance) VALUES (?, ?)',
        ['Bob', 500.00],
      );

      await tx.rollback();

      final result = await mysql.queryRaw(
        'SELECT COUNT(*) FROM test_accounts WHERE name = "Bob"',
      );
      expect(result.rows[0][0], 0);
    });

    test('transaction isolation - uncommitted changes not visible', () async {
      final tx = await mysql.beginTransaction();

      await tx.query(
        'INSERT INTO test_accounts (name, balance) VALUES (?, ?)',
        ['Charlie', 750.00],
      );

      final poolResult = await mysql.queryRaw(
        'SELECT COUNT(*) FROM test_accounts WHERE name = "Charlie"',
      );
      expect(poolResult.rows[0][0], 0);

      await tx.commit();

      final poolResultAfter = await mysql.queryRaw(
        'SELECT COUNT(*) FROM test_accounts WHERE name = "Charlie"',
      );
      expect(poolResultAfter.rows[0][0], 1);
    });

    test('multiple transactions can execute concurrently', () async {
      final tx1 = await mysql.beginTransaction();
      final tx2 = await mysql.beginTransaction();

      await tx1.query(
        'INSERT INTO test_accounts (name, balance) VALUES (?, ?)',
        ['Account1', 100],
      );
      await tx2.query(
        'INSERT INTO test_accounts (name, balance) VALUES (?, ?)',
        ['Account2', 200],
      );

      await tx1.commit();
      await tx2.commit();

      final result = await mysql.queryRaw(
        'SELECT name, balance FROM test_accounts ORDER BY name',
      );
      expect(result.rows.length, 2);
      expect(result.rows[0][0], 'Account1');
      expect(result.rows[1][0], 'Account2');
    });

    test('transaction maintains session variables', () async {
      final tx = await mysql.beginTransaction();

      await tx.queryRaw('SET @tx_var = 12345');

      final txResult = await tx.query('SELECT @tx_var');
      expect(txResult.rows[0][0], 12345);

      final poolResult = await mysql.query('SELECT @tx_var');
      expect(poolResult.rows[0][0], isNull);

      await tx.commit();
    });

    test('transaction with complex money transfer', () async {
      await mysql.insertBatch(
        'test_accounts',
        ['name', 'balance'],
        [
          ['Alice', 1000.00],
          ['Bob', 500.00],
        ],
      );

      final tx = await mysql.beginTransaction();

      await tx.query(
        'UPDATE test_accounts SET balance = balance - ? WHERE name = ?',
        [250.00, 'Alice'],
      );
      await tx.query(
        'UPDATE test_accounts SET balance = balance + ? WHERE name = ?',
        [250.00, 'Bob'],
      );

      await tx.commit();

      final result = await mysql.queryRaw(
        'SELECT name, balance FROM test_accounts ORDER BY name',
      );

      expect(result.rows[0][1], 750.00);
      expect(result.rows[1][1], 750.00);
    });

    test('transaction rollback after error', () async {
      await mysql.query(
        'INSERT INTO test_accounts (name, balance) VALUES (?, ?)',
        ['Original', 1000.00],
      );

      final tx = await mysql.beginTransaction();

      await tx.query('UPDATE test_accounts SET balance = ? WHERE name = ?', [
        500.00,
        'Original',
      ]);

      try {
        await tx.query('SELECT * FROM non_existent_table');
      } catch (e) {
        await tx.rollback();
      }

      final result = await mysql.queryRaw(
        'SELECT balance FROM test_accounts WHERE name = "Original"',
      );
      expect(result.rows[0][0], 1000.00);
    });

    test('transaction can use batch insert', () async {
      final tx = await mysql.beginTransaction();

      await tx.insertBatch(
        'test_accounts',
        ['name', 'balance'],
        [
          ['Batch1', 100],
          ['Batch2', 200],
          ['Batch3', 300],
        ],
      );

      final txResult = await tx.query('SELECT COUNT(*) FROM test_accounts');
      expect(txResult.rows[0][0], 3);

      await tx.commit();

      final poolResult = await mysql.queryRaw(
        'SELECT COUNT(*) FROM test_accounts',
      );
      expect(poolResult.rows[0][0], 3);
    });

    test('throws error after transaction is committed', () async {
      final tx = await mysql.beginTransaction();
      await tx.commit();

      expect(
        () async => await tx.query('SELECT 1'),
        throwsA(isA<MySQLException>()),
      );
    });

    test('throws error after transaction is rolled back', () async {
      final tx = await mysql.beginTransaction();
      await tx.rollback();

      expect(
        () async => await tx.query('SELECT 1'),
        throwsA(isA<MySQLException>()),
      );
    });

    test('releasing transaction connection twice does not crash', () async {
      final tx = await mysql.beginTransaction();
      await tx.commit();
      await tx.release();
    });
  });

  group('Integration Tests - Concurrent Operations', () {
    late MySqlPool mysql;

    setUpAll(() async {
      mysql = MySqlPool(
        MySqlConfig(
          host: '127.0.0.1',
          user: user,
          pass: pass,
          dbName: dbName,
          port: port,
          poolMax: 10,
        ),
      );

      await mysql.connect();
      await mysql.query('DROP TABLE IF EXISTS test_concurrent');
      await mysql.query('''
        CREATE TABLE test_concurrent (
          id INT AUTO_INCREMENT PRIMARY KEY,
          thread_id INT,
          value INT
        )
      ''');
    });

    tearDownAll(() async {
      if (mysql.isConnected) {
        await mysql.query('DROP TABLE IF EXISTS test_concurrent');
        await mysql.close();
      }
    });

    setUp(() async {
      await mysql.query('DELETE FROM test_concurrent');
    });

    test('concurrent inserts from multiple futures', () async {
      final futures = List.generate(10, (i) {
        return mysql.query(
          'INSERT INTO test_concurrent (thread_id, value) VALUES (?, ?)',
          [i, i * 100],
        );
      });

      await Future.wait(futures);

      final result = await mysql.query('SELECT COUNT(*) FROM test_concurrent');
      expect(result.rows[0][0], 10);
    });

    test('concurrent selects', () async {
      await mysql.insertBatch(
        'test_concurrent',
        ['thread_id', 'value'],
        [
          [1, 100],
          [2, 200],
          [3, 300],
        ],
      );

      final futures = List.generate(20, (i) {
        return mysql.query('SELECT SUM(value) FROM test_concurrent');
      });

      final results = await Future.wait(futures);

      for (final result in results) {
        expect(result.rows[0][0], 600);
      }
    });

    test('concurrent updates', () async {
      await mysql.insertBatch('test_concurrent', [
        'thread_id',
        'value',
      ], List.generate(10, (i) => [i, 0]));

      final futures = List.generate(10, (i) {
        return mysql.query(
          'UPDATE test_concurrent SET value = ? WHERE thread_id = ?',
          [i * 10, i],
        );
      });

      await Future.wait(futures);

      final result = await mysql.queryRaw(
        'SELECT SUM(value) FROM test_concurrent',
      );
      expect(result.rows[0][0], 450);
    });

    test('mixed concurrent operations', () async {
      final futures = <Future>[];

      for (var i = 0; i < 5; i++) {
        futures.add(
          mysql.query(
            'INSERT INTO test_concurrent (thread_id, value) VALUES (?, ?)',
            [i, i],
          ),
        );
      }

      for (var i = 0; i < 5; i++) {
        futures.add(mysql.query('SELECT COUNT(*) FROM test_concurrent'));
      }

      await Future.wait(futures);

      final result = await mysql.query('SELECT COUNT(*) FROM test_concurrent');
      expect(result.rows[0][0], 5);
    });

    test('concurrent prepared statements', () async {
      final stmt = await mysql.prepare(
        'INSERT INTO test_concurrent (thread_id, value) VALUES (?, ?)',
      );

      final futures = List.generate(10, (i) {
        return stmt.execute([i, i * 10]);
      });

      await Future.wait(futures);
      await stmt.release();

      final result = await mysql.query('SELECT COUNT(*) FROM test_concurrent');
      expect(result.rows[0][0], 10);
    });
  });

  group('Integration Tests - Edge Cases', () {
    late MySqlPool mysql;

    setUpAll(() async {
      mysql = MySqlPool(
        MySqlConfig(
          host: '127.0.0.1',
          user: user,
          pass: pass,
          dbName: dbName,
          port: port,
        ),
      );

      await mysql.connect();
      await mysql.query('DROP TABLE IF EXISTS test_edge');
      await mysql.query('''
        CREATE TABLE test_edge (
          id INT AUTO_INCREMENT PRIMARY KEY,
          data TEXT
        )
      ''');
    });

    tearDownAll(() async {
      if (mysql.isConnected) {
        await mysql.query('DROP TABLE IF EXISTS test_edge');
        await mysql.close();
      }
    });

    setUp(() async {
      await mysql.queryRaw('TRUNCATE TABLE test_edge');
    });

    test('handles very long strings', () async {
      final longString = 'A' * 10000;

      await mysql.query('INSERT INTO test_edge (data) VALUES (?)', [
        longString,
      ]);

      final result = await mysql.query('SELECT data FROM test_edge');
      expect(result.rows[0][0].toString().length, 10000);
    });

    test('handles empty parameter list', () async {
      final result = await mysql.query('SELECT 1 AS num', []);
      expect(result.rows[0][0].toString(), '1');
    });

    test('handles query with no results', () async {
      final result = await mysql.query(
        'SELECT * FROM test_edge WHERE id = ?',
        [999999],
      );

      expect(result.rows, isEmpty);
    });

    test('handles very small numbers', () async {
      await mysql.query('DROP TABLE IF EXISTS test_small');
      await mysql.query('CREATE TABLE test_small (val INT)');
      await mysql.query('INSERT INTO test_small (val) VALUES (?)', [0]);

      final result = await mysql.query('SELECT val FROM test_small');
      expect(result.rows, isNotEmpty);
      expect(result.rows[0][0], 0);
      await mysql.query('DROP TABLE test_small');
    });

    test('handles negative numbers', () async {
      await mysql.query('DROP TABLE IF EXISTS test_negative');
      await mysql.query('''
        CREATE TABLE test_negative (
          id INT AUTO_INCREMENT PRIMARY KEY,
          value INT
        )
      ''');

      await mysql.query('INSERT INTO test_negative (value) VALUES (?)', [
        -999,
      ]);

      final result = await mysql.query('SELECT value FROM test_negative');
      expect(result.rows[0][0], -999);

      await mysql.query('DROP TABLE test_negative');
    });

    test('handles whitespace-only strings', () async {
      await mysql.query('INSERT INTO test_edge (data) VALUES (?)', ['   ']);

      final result = await mysql.query('SELECT data FROM test_edge');
      expect(result.rows[0][0], '   ');
    });

    test('handles newlines and tabs in strings', () async {
      const text = 'Line1\nLine2\tTabbed';

      await mysql.query('INSERT INTO test_edge (data) VALUES (?)', [text]);

      final result = await mysql.query('SELECT data FROM test_edge');
      expect(result.rows[0][0], text);
    });

    test('handles SQL keywords as values', () async {
      await mysql.query('INSERT INTO test_edge (data) VALUES (?)', [
        'SELECT * FROM users WHERE id = 1',
      ]);

      final result = await mysql.query('SELECT data FROM test_edge');
      expect(result.rows[0][0], 'SELECT * FROM users WHERE id = 1');
    });

    test('handles strings with null bytes (FFI boundary check)', () async {
      final textWithNull = 'Start\x00Middle\x00End';

      await mysql.query('INSERT INTO test_edge (data) VALUES (?)', [
        textWithNull,
      ]);

      final result = await mysql.query('SELECT data FROM test_edge');
      expect(result.rows[0][0], textWithNull);
    });
  });

  group('Integration Tests - Connection Pool', () {
    test('creates pool with custom configuration', () async {
      final pool = MySqlPool(
        MySqlConfig(
          host: '127.0.0.1',
          user: user,
          pass: pass,
          dbName: dbName,
          poolMin: 2,
          poolMax: 5,
        ),
      );

      await pool.connect();
      expect(pool.isConnected, isTrue);
      await pool.close();
      expect(pool.isConnected, isFalse);
    });

    test('can reconnect after closing', () async {
      final pool = MySqlPool(
        MySqlConfig(
          host: '127.0.0.1',
          user: user,
          pass: pass,
          dbName: dbName,
        ),
      );

      await pool.connect();
      expect(pool.isConnected, isTrue);

      await pool.close();
      expect(pool.isConnected, isFalse);

      await pool.connect();
      expect(pool.isConnected, isTrue);

      final result = await pool.query('SELECT 1');
      expect(result.rows[0][0].toString(), '1');

      await pool.close();
    });

    test('throws error when using closed pool', () async {
      final pool = MySqlPool(
        MySqlConfig(
          host: '127.0.0.1',
          user: user,
          pass: pass,
          dbName: dbName,
        ),
      );

      await pool.connect();
      await pool.close();

      expect(
        () async => await pool.query('SELECT 1'),
        throwsA(isA<MySQLException>()),
      );
    });

    test('ensureDatabase creates database if not exists', () async {
      final pool = MySqlPool(
        MySqlConfig(
          host: '127.0.0.1',
          user: user,
          pass: pass,
          dbName: 'test_ensure_db_${DateTime.now().millisecondsSinceEpoch}',
        ),
      );

      await pool.ensureDatabase();
      await pool.connect();
      expect(pool.isConnected, isTrue);
      await pool.close();
    });

    test('connection failure throws exception', () async {
      final badPool = MySqlPool(
        MySqlConfig(
          host: '127.0.0.1',
          user: user,
          pass: 'wrong_pass',
          dbName: dbName,
          port: 9999,
        ),
      );
      expect(
        () async => await badPool.connect(),
        throwsA(isA<MySQLException>()),
      );
    });
  });

  group('Integration Tests - Dedicated Connections', () {
    late MySqlPool mysql;

    setUpAll(() async {
      mysql = MySqlPool(
        MySqlConfig(
          host: '127.0.0.1',
          user: user,
          pass: pass,
          dbName: dbName,
          port: port,
        ),
      );

      await mysql.connect();
      await mysql.query('DROP TABLE IF EXISTS test_dedicated_conn');
      await mysql.query('''
        CREATE TABLE test_dedicated_conn (
          id INT AUTO_INCREMENT PRIMARY KEY,
          val VARCHAR(50)
        )
      ''');
    });

    tearDownAll(() async {
      if (mysql.isConnected) {
        await mysql.query('DROP TABLE IF EXISTS test_dedicated_conn');
        await mysql.close();
      }
    });

    setUp(() async {
      await mysql.query('DELETE FROM test_dedicated_conn');
    });

    test('getConnection returns a usable connection', () async {
      final conn = await mysql.getConnection();
      expect(conn.isTransaction, isFalse);

      await conn.query('INSERT INTO test_dedicated_conn (val) VALUES (?)', [
        'test_val',
      ]);
      final result = await conn.query('SELECT val FROM test_dedicated_conn');

      expect(result.rows.length, 1);
      expect(result.rows[0][0], 'test_val');

      await conn.release();
    });

    test('commit on dedicated connection throws exception', () async {
      final conn = await mysql.getConnection();
      expect(conn.isTransaction, isFalse);

      expect(() async => await conn.commit(), throwsA(isA<MySQLException>()));

      await conn.release();
    });

    test('rollback on dedicated connection throws exception', () async {
      final conn = await mysql.getConnection();
      expect(conn.isTransaction, isFalse);

      expect(() async => await conn.rollback(), throwsA(isA<MySQLException>()));

      await conn.release();
    });

    test('throws error after connection is released', () async {
      final conn = await mysql.getConnection();
      await conn.release();

      expect(
        () async => await conn.query('SELECT 1'),
        throwsA(isA<MySQLException>()),
      );
    });
  });
}
