/// MySQL column type identifiers from the wire protocol.
class MySqlColumnType {
  static const int decimal = 0;
  static const int tiny = 1;
  static const int short = 2;
  static const int long = 3;
  static const int float = 4;
  static const int double_ = 5;
  static const int null_ = 6;
  static const int timestamp = 7;
  static const int longlong = 8;
  static const int int24 = 9;
  static const int date = 10;
  static const int time = 11;
  static const int datetime = 12;
  static const int year = 13;
  static const int newDate = 14;
  static const int varchar = 15;
  static const int bit = 16;
  static const int json = 245;
  static const int newDecimal = 246;
  static const int enum_ = 247;
  static const int set_ = 248;
  static const int tinyBlob = 249;
  static const int mediumBlob = 250;
  static const int longBlob = 251;
  static const int blob = 252;
  static const int varString = 253;
  static const int string = 254;
  static const int geometry = 255;
}

/// MySQL charset identifier for binary data.
const int mysqlCharsetBinary = 63;

/// Type tags used in the Dart-to-Rust parameter encoding protocol.
class SqlParamType {
  static const int nullValue = 0;
  static const int intValue = 1;
  static const int floatValue = 2;
  static const int stringValue = 3;
  static const int blobValue = 4;
}
