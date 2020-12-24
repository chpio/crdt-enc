{
    description = "crdt-enc";

    inputs = {
        nixpkgs.url = github:NixOS/nixpkgs;
        flake-utils.url = "github:numtide/flake-utils";
    };

    outputs = { self, flake-utils, nixpkgs }:
        flake-utils.lib.eachDefaultSystem (system:
            let
                pkgs = import nixpkgs { inherit system; };
            in {
                devShell = pkgs.stdenv.mkDerivation {
                    name = "crdt-enc";
                    buildInputs = [
                        pkgs.gpgme
                        pkgs.libsodium

                        pkgs.cargo
                        pkgs.rustc
                        pkgs.rustfmt
                        pkgs.rust-analyzer
                    ];
                };
            }
        );
}
