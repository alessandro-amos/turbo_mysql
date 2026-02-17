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

  group('MySQL to Dart Type Mapping Tests', () {
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
      await mysql.query('DROP TABLE IF EXISTS type_mapping_test');
      await mysql.query('''
        CREATE TABLE type_mapping_test (
          id INT AUTO_INCREMENT PRIMARY KEY,
          col_tinyint TINYINT,
          col_smallint SMALLINT,
          col_mediumint MEDIUMINT,
          col_int INT,
          col_bigint BIGINT,
          col_float FLOAT,
          col_double DOUBLE,
          col_decimal DECIMAL(10,2),
          col_date DATE,
          col_datetime DATETIME,
          col_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          col_time TIME,
          col_year YEAR,
          col_char CHAR(10),
          col_varchar VARCHAR(50),
          col_text TEXT,
          col_blob BLOB,
          col_json JSON,
          col_enum ENUM('A', 'B', 'C'),
          col_set SET('X', 'Y', 'Z'),
          col_bit BIT(8)
        )
      ''');
    });

    tearDownAll(() async {
      if (mysql.isConnected) {
        await mysql.query('DROP TABLE IF EXISTS type_mapping_test');
        await mysql.close();
      }
    });

    setUp(() async {
      await mysql.query('DELETE FROM type_mapping_test');
    });

    test('validates correct Dart types for MySQL types', () async {
      final blobData = Uint8List.fromList([1, 2, 3, 4, 5]);

      await mysql.query(
        '''
        INSERT INTO type_mapping_test (
          col_tinyint, col_smallint, col_mediumint, col_int, col_bigint,
          col_float, col_double, col_decimal,
          col_date, col_datetime, col_time, col_year,
          col_char, col_varchar, col_text, col_blob, col_json,
          col_enum, col_set, col_bit
        ) VALUES (
          ?, ?, ?, ?, ?,
          ?, ?, ?,
          ?, ?, ?, ?,
          ?, ?, ?, ?, ?,
          ?, ?, b'10101010'
        )
      ''',
        [
          127,
          32767,
          8388607,
          2147483647,
          BigInt.parse('9223372036854775807'),
          3.14,
          2.718281828,
          99.99,
          DateTime(2024, 1, 1),
          DateTime(2024, 1, 1, 12, 0, 0),
          '12:30:45',
          2024,
          'fixed',
          'variable',
          'long text data',
          blobData,
          {'key': 'value'},
          'B',
          'X,Z',
        ],
      );

      final result = await mysql.queryRaw(
        'SELECT * FROM type_mapping_test LIMIT 1',
      );
      final row = result.asMaps.first;

      expect(row['col_tinyint'], isA<int>());
      expect(row['col_smallint'], isA<int>());
      expect(row['col_mediumint'], isA<int>());
      expect(row['col_int'], isA<int>());
      expect(row['col_bigint'], isA<int>());

      expect(row['col_float'], isA<double>());
      expect(row['col_double'], isA<double>());
      expect(row['col_decimal'], isA<double>());

      expect(row['col_date'], isA<DateTime>());
      expect(row['col_datetime'], isA<DateTime>());
      expect(row['col_timestamp'], isA<DateTime>());

      expect(row['col_time'], isA<String>());

      expect(row['col_year'], isA<int>());

      expect(row['col_char'], isA<String>());
      expect(row['col_varchar'], isA<String>());
      expect(row['col_text'], isA<String>());
      expect(row['col_json'], isA<Map<String, dynamic>>());
      expect(row['col_enum'], isA<String>());
      expect(row['col_set'], isA<String>());

      expect(row['col_blob'], isA<Uint8List>());
      expect(row['col_bit'], isA<Uint8List>());
    });

    test('validates null values mapping', () async {
      await mysql.query(
        'INSERT INTO type_mapping_test (col_int) VALUES (NULL)',
      );

      final result = await mysql.queryRaw(
        'SELECT * FROM type_mapping_test LIMIT 1',
      );
      final row = result.asMaps.first;

      expect(row['col_tinyint'], isNull);
      expect(row['col_smallint'], isNull);
      expect(row['col_float'], isNull);
      expect(row['col_decimal'], isNull);
      expect(row['col_date'], isNull);
      expect(row['col_datetime'], isNull);
      expect(row['col_time'], isNull);
      expect(row['col_char'], isNull);
      expect(row['col_varchar'], isNull);
      expect(row['col_blob'], isNull);
      expect(row['col_json'], isNull);
      expect(row['col_bit'], isNull);
    });
  });
}
