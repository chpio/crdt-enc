let
    pkgs = import <nixpkgs> {};
in
    pkgs.stdenv.mkDerivation {
        name = "crdt-enc";
        buildInputs = [
            pkgs.gpgme
            pkgs.libsodium

            pkgs.cargo
            pkgs.rustc
            pkgs.rustfmt
            pkgs.rust-analyzer
        ];
    }
