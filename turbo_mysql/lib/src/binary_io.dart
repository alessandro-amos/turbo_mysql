import 'dart:convert';
import 'dart:ffi';
import 'dart:typed_data';

/// A helper class to read binary data from a memory pointer.
///
/// Provides methods to read primitive types and length-prefixed blobs from
/// a [Pointer<Uint8>] typically received from FFI callbacks.
class BinaryReader {
  final ByteData _view;
  int _offset = 0;
  final Uint8List _rawBytes;

  /// Creates a [BinaryReader] copying data from a native pointer.
  ///
  /// The data is immediately copied into Dart-managed memory, so the
  /// native buffer can be freed at any time after construction.
  BinaryReader(Pointer<Uint8> ptr, int len)
      : _rawBytes = Uint8List.fromList(ptr.asTypedList(len)),
        _view = ByteData.sublistView(
          Uint8List.fromList(ptr.asTypedList(len)),
        );

  BinaryReader.fromBytes(Uint8List bytes)
      : _rawBytes = bytes,
        _view = ByteData.sublistView(bytes);

  /// Reads an 8-bit unsigned integer and advances the offset by 1.
  int readUint8() {
    final v = _view.getUint8(_offset);
    _offset += 1;
    return v;
  }

  /// Reads a 16-bit unsigned integer (little-endian) and advances the offset by 2.
  int readUint16() {
    final v = _view.getUint16(_offset, Endian.little);
    _offset += 2;
    return v;
  }

  /// Reads a 32-bit unsigned integer (little-endian) and advances the offset by 4.
  int readUint32() {
    final v = _view.getUint32(_offset, Endian.little);
    _offset += 4;
    return v;
  }

  /// Reads a 64-bit unsigned integer (little-endian) and advances the offset by 8.
  int readUint64() {
    final v = _view.getUint64(_offset, Endian.little);
    _offset += 8;
    return v;
  }

  /// Reads a 64-bit signed integer (little-endian) and advances the offset by 8.
  int readInt64() {
    final v = _view.getInt64(_offset, Endian.little);
    _offset += 8;
    return v;
  }

  /// Reads a 64-bit floating-point number (little-endian) and advances the offset by 8.
  double readFloat64() {
    final v = _view.getFloat64(_offset, Endian.little);
    _offset += 8;
    return v;
  }

  /// Reads a length-prefixed raw byte array.
  Uint8List readBlob() {
    final len = readUint32();
    if (len == 0) return Uint8List(0);
    final bytes = _rawBytes.sublist(_offset, _offset + len);
    _offset += len;
    return bytes;
  }

  /// Reads a length-prefixed UTF-8 string.
  String readString() {
    final len = readUint32();
    if (len == 0) return '';
    final str = utf8.decode(
      _rawBytes.sublist(_offset, _offset + len),
      allowMalformed: true,
    );
    _offset += len;
    return str;
  }
}

/// A helper class to write binary data into a growing buffer.
///
/// Uses [BytesBuilder] to construct binary payloads to be sent via FFI.
class BinaryWriter {
  final BytesBuilder _builder = BytesBuilder();

  /// Writes an 8-bit unsigned integer to the buffer.
  void writeUint8(int v) {
    _builder.addByte(v);
  }

  /// Writes a 32-bit unsigned integer (little-endian) to the buffer.
  void writeUint32(int v) {
    final b = ByteData(4)..setUint32(0, v, Endian.little);
    _builder.add(b.buffer.asUint8List());
  }

  /// Writes a 64-bit signed integer (little-endian) to the buffer.
  void writeInt64(int v) {
    final b = ByteData(8)..setInt64(0, v, Endian.little);
    _builder.add(b.buffer.asUint8List());
  }

  /// Writes a 64-bit floating-point number (little-endian) to the buffer.
  void writeFloat64(double v) {
    final b = ByteData(8)..setFloat64(0, v, Endian.little);
    _builder.add(b.buffer.asUint8List());
  }

  /// Writes a raw byte array prefixed with its length as a [Uint32].
  void writeBlob(List<int> bytes) {
    writeUint32(bytes.length);
    _builder.add(bytes);
  }

  /// Writes a UTF-8 string prefixed with its byte length as a [Uint32].
  void writeString(String s) {
    writeBlob(utf8.encode(s));
  }

  /// Returns the accumulated bytes as a [Uint8List].
  Uint8List toBytes() => _builder.toBytes();
}
