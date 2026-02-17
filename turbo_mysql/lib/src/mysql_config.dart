import 'dart:core';

/// Configuration options for connecting to a MySQL database.
class MySqlConfig {
  /// The hostname or IP address of the database server.
  final String host;

  /// The username for authentication.
  final String user;

  /// The password for authentication.
  final String pass;

  /// The name of the database to connect to.
  final String dbName;

  /// The port number of the database server.
  final int port;

  /// The minimum number of connections in the pool.
  final int poolMin;

  /// The maximum number of connections in the pool.
  final int poolMax;

  /// A list of queries to execute when a connection is initialized.
  final List<String> init;

  /// A list of queries to execute when the pool is setup.
  final List<String> setup;

  /// The TCP keepalive interval in seconds.
  final int? tcpKeepalive;

  /// Whether to disable Nagle's algorithm (TCP_NODELAY).
  final bool? tcpNodelay;

  /// The maximum idle time for a connection in seconds.
  final int? connTtl;

  /// The absolute maximum lifetime for a connection in seconds.
  final int? absConnTtl;

  /// Jitter applied to the absolute connection TTL.
  final int? absConnTtlJitter;

  /// The size of the prepared statement cache.
  final int? stmtCacheSize;

  /// Whether SSL is required for the connection.
  final bool? requireSsl;

  /// Whether to verify the server's Certificate Authority.
  final bool? verifyCa;

  /// Whether to verify the server's identity.
  final bool? verifyIdentity;

  /// Whether to prefer a Unix domain socket connection.
  final bool? preferSocket;

  /// The path to the Unix domain socket.
  final String? socket;

  /// The compression algorithm to use.
  final String? compression;

  /// The maximum allowed packet size in bytes.
  final int? maxAllowedPacket;

  /// The wait timeout for the connection in seconds.
  final int? waitTimeout;

  /// Whether to use secure authentication.
  final bool? secureAuth;

  /// Whether to return the number of found rows instead of affected rows.
  final bool? clientFoundRows;

  /// Whether to enable the cleartext authentication plugin.
  final bool? enableCleartextPlugin;

  /// Creates a new [MySqlConfig] with the given settings.
  const MySqlConfig({
    required this.host,
    required this.user,
    required this.pass,
    this.dbName = '',
    this.port = 3306,
    this.poolMin = 1,
    this.poolMax = 10,
    this.init = const [],
    this.setup = const [],
    this.tcpKeepalive,
    this.tcpNodelay = true,
    this.connTtl,
    this.absConnTtl,
    this.absConnTtlJitter,
    this.stmtCacheSize,
    this.requireSsl,
    this.verifyCa,
    this.verifyIdentity,
    this.preferSocket,
    this.socket,
    this.compression,
    this.maxAllowedPacket,
    this.waitTimeout,
    this.secureAuth,
    this.clientFoundRows,
    this.enableCleartextPlugin,
  });

  /// Creates a copy of this configuration, replacing specified fields with new values.
  MySqlConfig copyWith({
    String? host,
    String? user,
    String? pass,
    String? dbName,
    int? port,
    int? poolMin,
    int? poolMax,
    List<String>? init,
    List<String>? setup,
    int? tcpKeepalive,
    bool? tcpNodelay,
    int? connTtl,
    int? absConnTtl,
    int? absConnTtlJitter,
    int? stmtCacheSize,
    bool? requireSsl,
    bool? verifyCa,
    bool? verifyIdentity,
    bool? preferSocket,
    String? socket,
    String? compression,
    int? maxAllowedPacket,
    int? waitTimeout,
    bool? secureAuth,
    bool? clientFoundRows,
    bool? enableCleartextPlugin,
  }) {
    return MySqlConfig(
      host: host ?? this.host,
      user: user ?? this.user,
      pass: pass ?? this.pass,
      dbName: dbName ?? this.dbName,
      port: port ?? this.port,
      poolMin: poolMin ?? this.poolMin,
      poolMax: poolMax ?? this.poolMax,
      init: init ?? this.init,
      setup: setup ?? this.setup,
      tcpKeepalive: tcpKeepalive ?? this.tcpKeepalive,
      tcpNodelay: tcpNodelay ?? this.tcpNodelay,
      connTtl: connTtl ?? this.connTtl,
      absConnTtl: absConnTtl ?? this.absConnTtl,
      absConnTtlJitter: absConnTtlJitter ?? this.absConnTtlJitter,
      stmtCacheSize: stmtCacheSize ?? this.stmtCacheSize,
      requireSsl: requireSsl ?? this.requireSsl,
      verifyCa: verifyCa ?? this.verifyCa,
      verifyIdentity: verifyIdentity ?? this.verifyIdentity,
      preferSocket: preferSocket ?? this.preferSocket,
      socket: socket ?? this.socket,
      compression: compression ?? this.compression,
      maxAllowedPacket: maxAllowedPacket ?? this.maxAllowedPacket,
      waitTimeout: waitTimeout ?? this.waitTimeout,
      secureAuth: secureAuth ?? this.secureAuth,
      clientFoundRows: clientFoundRows ?? this.clientFoundRows,
      enableCleartextPlugin:
          enableCleartextPlugin ?? this.enableCleartextPlugin,
    );
  }

  static String _percentEncode(String input) {
    final encoded = StringBuffer();
    final bytes = input.codeUnits;
    for (final byte in bytes) {
      if ((byte >= 0x30 && byte <= 0x39) ||
          (byte >= 0x41 && byte <= 0x5A) ||
          (byte >= 0x61 && byte <= 0x7A) ||
          byte == 0x2D ||
          byte == 0x2E ||
          byte == 0x5F ||
          byte == 0x7E) {
        encoded.writeCharCode(byte);
      } else {
        encoded.write(
          '%${byte.toRadixString(16).toUpperCase().padLeft(2, '0')}',
        );
      }
    }
    return encoded.toString();
  }

  /// Converts this configuration into a MySQL connection string URI.
  String toConnectionString() {
    final encodedUser = _percentEncode(user);
    final encodedPass = _percentEncode(pass);
    final dbPart = dbName.isNotEmpty ? '/$dbName' : '';

    final queryParams = <String>[];

    queryParams.add('pool_min=$poolMin');
    queryParams.add('pool_max=$poolMax');

    if (tcpNodelay != null) queryParams.add('tcp_nodelay=$tcpNodelay');
    if (tcpKeepalive != null) queryParams.add('tcp_keepalive=$tcpKeepalive');
    if (connTtl != null) queryParams.add('conn_ttl=$connTtl');
    if (absConnTtl != null) queryParams.add('abs_conn_ttl=$absConnTtl');
    if (absConnTtlJitter != null)
      queryParams.add('abs_conn_ttl_jitter=$absConnTtlJitter');
    if (stmtCacheSize != null)
      queryParams.add('stmt_cache_size=$stmtCacheSize');
    if (requireSsl != null) queryParams.add('require_ssl=$requireSsl');
    if (verifyCa != null) queryParams.add('verify_ca=$verifyCa');
    if (verifyIdentity != null)
      queryParams.add('verify_identity=$verifyIdentity');
    if (preferSocket != null) queryParams.add('prefer_socket=$preferSocket');
    if (socket != null) queryParams.add('socket=${_percentEncode(socket!)}');
    if (compression != null)
      queryParams.add('compression=${_percentEncode(compression!)}');
    if (maxAllowedPacket != null)
      queryParams.add('max_allowed_packet=$maxAllowedPacket');
    if (waitTimeout != null) queryParams.add('wait_timeout=$waitTimeout');
    if (secureAuth != null) queryParams.add('secure_auth=$secureAuth');
    if (clientFoundRows != null)
      queryParams.add('client_found_rows=$clientFoundRows');
    if (enableCleartextPlugin != null)
      queryParams.add('enable_cleartext_plugin=$enableCleartextPlugin');

    for (final initQuery in init) {
      queryParams.add('init=${_percentEncode(initQuery)}');
    }

    for (final setupQuery in setup) {
      queryParams.add('setup=${_percentEncode(setupQuery)}');
    }

    final queryString = queryParams.isNotEmpty
        ? '?${queryParams.join('&')}'
        : '';
    return 'mysql://$encodedUser:$encodedPass@$host:$port$dbPart$queryString';
  }
}
