import 'dart:convert';
import 'dart:ffi';
import 'dart:typed_data';
import 'package:ffi/ffi.dart';
import 'mysql_protocol.dart';
import 'binary_io.dart';

/// Utilities for converting between Dart objects and MySQL wire formats.
class DataConverter {
  /// Decodes a raw byte buffer into a Dart object based on the MySQL column type.
  static dynamic decodeValue(Uint8List bytes, int colType, int charset) {
    final view = ByteData.sublistView(bytes);

    switch (colType) {
      case MySqlColumnType.tiny:
      case MySqlColumnType.short:
      case MySqlColumnType.long:
      case MySqlColumnType.longlong:
      case MySqlColumnType.int24:
      case MySqlColumnType.year:
        if (bytes.isEmpty) return null;
        if (bytes.length == 8) return view.getInt64(0, Endian.little);
        return int.tryParse(utf8.decode(bytes, allowMalformed: true));
      case MySqlColumnType.float:
      case MySqlColumnType.double_:
        if (bytes.isEmpty) return null;
        if (bytes.length == 8) return view.getFloat64(0, Endian.little);
        return double.tryParse(utf8.decode(bytes, allowMalformed: true));
      case MySqlColumnType.decimal:
      case MySqlColumnType.newDecimal:
        if (bytes.isEmpty) return null;
        return double.tryParse(utf8.decode(bytes, allowMalformed: true));
      case MySqlColumnType.timestamp:
      case MySqlColumnType.date:
      case MySqlColumnType.datetime:
      case MySqlColumnType.newDate:
        if (bytes.isEmpty) return null;
        final str = utf8.decode(bytes, allowMalformed: true);
        try {
          return DateTime.parse(str.replaceFirst(' ', 'T'));
        } catch (s) {
          return str;
        }

      case MySqlColumnType.json:
        if (bytes.isEmpty) return null;
        final str = utf8.decode(bytes, allowMalformed: true);
        try {
          return jsonDecode(str);
        } catch (_) {
          return str;
        }
      case MySqlColumnType.enum_:
      case MySqlColumnType.set_:
      case MySqlColumnType.varchar:
      case MySqlColumnType.varString:
      case MySqlColumnType.string:
      case MySqlColumnType.time:
        if (bytes.isEmpty) return '';
        return utf8.decode(bytes, allowMalformed: true);
      case MySqlColumnType.bit:
      case MySqlColumnType.geometry:
        return bytes;
      case MySqlColumnType.tinyBlob:
      case MySqlColumnType.mediumBlob:
      case MySqlColumnType.longBlob:
      case MySqlColumnType.blob:
        if (bytes.isEmpty) return bytes;
        return charset == mysqlCharsetBinary
            ? bytes
            : utf8.decode(bytes, allowMalformed: true);
      default:
        return charset == mysqlCharsetBinary
            ? bytes
            : utf8.decode(bytes, allowMalformed: true);
    }
  }

  /// Encodes a list of Dart parameters into a native memory block for Rust.
  static Pointer<Uint8> encodeParams(
    List<dynamic> params,
    Arena arena,
    BinaryWriter writer,
  ) {
    writer.writeUint32(params.length);
    for (final param in params) {
      writeParam(writer, param);
    }
    final bytes = writer.toBytes();
    final ptr = arena.allocate<Uint8>(bytes.length);
    ptr.asTypedList(bytes.length).setAll(0, bytes);
    return ptr;
  }

  /// Encodes a single parameter value into the binary writer.
  static void writeParam(BinaryWriter writer, dynamic param) {
    if (param == null) {
      writer.writeUint8(SqlParamType.nullValue);
    } else if (param is int) {
      writer.writeUint8(SqlParamType.intValue);
      writer.writeInt64(param);
    } else if (param is double) {
      writer.writeUint8(SqlParamType.floatValue);
      writer.writeFloat64(param);
    } else if (param is bool) {
      writer.writeUint8(SqlParamType.intValue);
      writer.writeInt64(param ? 1 : 0);
    } else if (param is DateTime) {
      writer.writeUint8(SqlParamType.stringValue);
      final str = param.toIso8601String().replaceAll('T', ' ').substring(0, 19);
      writer.writeString(str);
    } else if (param is BigInt) {
      writer.writeUint8(SqlParamType.stringValue);
      writer.writeString(param.toString());
    } else if (param is String) {
      writer.writeUint8(SqlParamType.stringValue);
      writer.writeString(param);
    } else if (param is Uint8List) {
      writer.writeUint8(SqlParamType.blobValue);
      writer.writeBlob(param);
    } else if (param is List<int>) {
      writer.writeUint8(SqlParamType.blobValue);
      writer.writeBlob(param);
    } else if (param is Map || param is Iterable) {
      writer.writeUint8(SqlParamType.stringValue);
      writer.writeString(jsonEncode(param));
    } else {
      writer.writeUint8(SqlParamType.stringValue);
      writer.writeString(param.toString());
    }
  }
}
