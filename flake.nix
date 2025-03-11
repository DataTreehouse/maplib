{
  inputs = {
    nixpkgs.url = "nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";

    crane.url = "github:ipetkov/crane";

    fenix.url = "github:nix-community/fenix";
    fenix.inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs = { self, flake-utils, nixpkgs, crane, fenix, ... }@inputs:
    flake-utils.lib.eachDefaultSystem (system: let
      pkgs = nixpkgs.legacyPackages.${system};
      inherit (pkgs) lib;

      fenixSet = fenix.packages.${system}.complete;
      inherit (fenixSet) toolchain;

      craneLib = (crane.mkLib pkgs).overrideToolchain toolchain;

      rustPlatform = pkgs.makeRustPlatform {
        cargo = toolchain;
        rustc = toolchain;
      };

      root = ./.;
      src = lib.fileset.toSource {
        inherit root;
        fileset = lib.fileset.unions [
          (craneLib.fileset.commonCargoSources root)
          (lib.fileset.fileFilter (file: file.hasExt "md") root)
          ./py_maplib/LICENSE
          ./py_maplib
        ];
      };

      cargoVendorDir = craneLib.vendorCargoDeps { inherit src; };
      cargoArtifacts = craneLib.buildDepsOnly { inherit src; };

      python = let
        packageOverrides = self: super: {
          maplib = self.callPackage ./nix/py_maplib {
            inherit src
              craneLib cargoVendorDir
              rustPlatform;
          };
        };
      in pkgs.python3.override {inherit packageOverrides; self = python;};
    in {
      packages = rec {
        default = py_maplib;
        py_maplib = python.pkgs.maplib;
        python-env = python.withPackages (ps: [ ps.maplib ps.polars ps.rdflib ]);
      };
      legacyPackages.python = python;
      devShells.default = craneLib.devShell {
        inputsFrom = [ self.packages.${system}.py_maplib ];

        # https://github.com/tikv/jemallocator/pull/116
        env.CFLAGS = "-Wno-error=int-conversion";

        packages = [
          pkgs.cargo-audit
          pkgs.cargo-deny
          pkgs.cargo-vet

          fenixSet.rust-analyzer
        ];
      };
      checks = {
        py_pytest = self.legacyPackages.${system}.python.pkgs.callPackage ./nix/py_maplib/pytest.nix {
          src = lib.fileset.toSource {
            root = ./.;
            fileset = lib.fileset.unions [
              ./py_maplib/tests
            ];
          };
        };
        cargo_test = craneLib.cargoTest {
          inherit src cargoArtifacts;
          inherit (craneLib.crateNameFromCargoToml { cargoToml = ./py_maplib/Cargo.toml; }) pname version;
        };
      };
      apps = rec {
        python-env = {
          type = "app";
          program = "${self.packages.${system}.python-env}/bin/python";
        };
      };
    }
  );
}
