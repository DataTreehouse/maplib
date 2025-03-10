{
  src,
  craneLib,
  cargoVendorDir,
  lib,
  callPackage,
  buildPythonPackage,
  pythonImportsCheckHook,
  pytestCheckHook,
  rustPlatform,
  polars,
  pyarrow,
}:
let
  cargoName = craneLib.crateNameFromCargoToml {
    cargoToml = src + /py_maplib/Cargo.toml;
  };
in
buildPythonPackage rec {
  pname = "maplib";
  inherit (cargoName) version;
  pyproject = true;

  inherit src cargoVendorDir;

  nativeBuildInputs = [
    # This is wrong, but there is an upstream bug with
    # makeRustPlatform and its buildHooks.
    # This probably breaks cross-compilation
    rustPlatform.rust.rustc
    rustPlatform.rust.cargo

    craneLib.configureCargoCommonVarsHook
    craneLib.configureCargoVendoredDepsHook
    rustPlatform.maturinBuildHook

    pythonImportsCheckHook
    # pytestCheckHook
  ];

  propagatedBuildInputs = [
    polars
    pyarrow
  ];

  buildAndTestSubdir = "py_maplib";

  passthru.tests = {
    pytest = callPackage ./pytest.nix { inherit src; };
  };
}
