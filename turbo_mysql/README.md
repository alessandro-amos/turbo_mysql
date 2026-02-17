# turbo_mysql

A high-performance MySQL client for Dart, powered by Rust via FFI. It provides a robust connection pool, prepared statements, and efficient batch operations.

## Features

- [x] Connection pooling
- [x] Simple text queries (`queryRaw`)
- [x] Parameterized queries with automatic type binding (`query`)
- [x] Prepared statements (`prepare`)
- [x] Statement Caching (`stmtCacheSize`)
- [x] Batch inserts (`insertBatch`)
- [x] Batch upserts / ON DUPLICATE KEY UPDATE (`upsertBatch`)
- [x] Transactions (`beginTransaction`, `commit`, `rollback`)
- [x] Extensive Data Type Support (JSON, BLOB, ENUM, SET, DateTime, etc.)
- [x] SSL/TLS Support (`requireSsl`, `verifyCa`, `verifyIdentity`)
- [x] Connection Lifecycle Management (TTL, Absolute TTL with Jitter)
- [x] Network & Protocol Tuning (TCP Keepalive, Nodelay, Compression, Max Allowed Packet)
- [x] Pre/Post Connection Hooks (`init` and `setup` queries)
- [x] Unix Domain Sockets (`preferSocket`, `socket`)
- [x] Secure Auth & Cleartext Plugin support
- [x] Native Rust performance via FFI

## Installation

Add `turbo_mysql` to your `pubspec.yaml` dependencies:

```yaml
dependencies:
  turbo_mysql: latest_version

```

## Examples

### Advanced Configuration

```dart
import 'package:turbo_mysql/turbo_mysql.dart';

final pool = MySqlPool(
  MySqlConfig(
    host: '127.0.0.1',
    user: 'root',
    pass: 'password',
    dbName: 'my_database',
    poolMin: 2,
    poolMax: 20,
    tcpKeepalive: 10000,
    connTtl: 60000,
    absConnTtl: 120000,
    absConnTtlJitter: 5000,
    stmtCacheSize: 100,
    requireSsl: true,
    compression: 'fast',
    init: ['SET NAMES utf8mb4', 'SET time_zone = "+00:00"'],
    setup: ['SET SESSION sql_mode = "STRICT_ALL_TABLES"'],
  ),
);

await pool.connect();

```

### Simple and Parameterized Queries

```dart
await pool.queryRaw('SELECT id, name FROM users LIMIT 10');
await pool.query(
  'SELECT * FROM users WHERE age > ? AND is_active = ?',
  [18, true],
);

```

### Prepared Statements

```dart
final stmt = await pool.prepare('INSERT INTO metrics (cpu, memory) VALUES (?, ?)');

await stmt.query([45.2, 1024]);
await stmt.query([50.1, 2048]);

await stmt.release();

```

### Batch Inserts and Upserts

```dart
final columns = ['name', 'age'];
final rows = [
  ['Alice', 28],
  ['Bob', 34],
  ['Charlie', 22],
];

final insertedCount = await pool.insertBatch('users', columns, rows);

final upsertedCount = await pool.upsertBatch('users', columns, rows);

```

### Transactions

```dart
final tx = await pool.beginTransaction();

try {
  await tx.query('UPDATE accounts SET balance = balance - ? WHERE id = ?', [100, 1]);
  await tx.query('UPDATE accounts SET balance = balance + ? WHERE id = ?', [100, 2]);
  
  await tx.commit();
} catch (e) {
  await tx.rollback();
}

```