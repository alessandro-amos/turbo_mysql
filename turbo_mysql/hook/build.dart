import 'package:code_assets/code_assets.dart';
import 'package:hooks/hooks.dart';

void main(List<String> args) async {
  await build(args, (input, output) async {
    final packageName = input.packageName;
    final assets = _getAssets(
      input,
      input.config.code.targetOS,
      input.config.code.targetArchitecture,
      packageName,
    );

    if (assets != null) {
      output.assets.code.addAll(assets);
    }
  });
}

List<CodeAsset>? _getAssets(
  BuildInput input,
  OS os,
  Architecture? arch,
  String packageName,
) {
  String? libPath;

  if (os == OS.macOS) {
    if (arch == Architecture.arm64) {
      libPath = 'native/macos-arm-64/libturbo_mysql_core.dylib';
    } else if (arch == Architecture.x64) {
      libPath = 'native/macos-x64/libturbo_mysql_core.dylib';
    }
  } else if (os == OS.linux) {
    if (arch == Architecture.x64) {
      libPath = 'native/linux-x64/libturbo_mysql_core.so';
    } else if (arch == Architecture.arm64) {
      libPath = 'native/linux-arm-64/libturbo_mysql_core.so';
    }
  } else if (os == OS.windows) {
    if (arch == Architecture.x64) {
      libPath = 'native/windows-x64/turbo_mysql_core.dll';
    }
  }

  if (libPath != null) {
    return [
      CodeAsset(
        package: packageName,
        name: 'turbo_mysql_core',
        linkMode: DynamicLoadingBundled(),
        file: input.packageRoot.resolve(libPath),
      ),
    ];
  }

  return null;
}
