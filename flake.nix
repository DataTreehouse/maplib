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
      inherit (fenix.packages.${system}.minimal) toolchain;
      
      craneLib = (crane.mkLib pkgs).overrideToolchain fenix.packages.${system}.minimal.toolchain;

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
          (./py_maplib/LICENSE)
        ];
      };

      cargoVendorDir = craneLib.vendorCargoDeps { inherit src; };
    in {
      packages = rec {
        py_maplib = pkgs.python3Packages.callPackage ./nix/py_maplib.nix {
          inherit
            src
            craneLib cargoVendorDir
            rustPlatform;
        };
      };
    }
  );
}
