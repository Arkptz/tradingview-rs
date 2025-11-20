{
  inputs = {
    nixpkgs.url = "nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    naersk = {
      url = "github:nix-community/naersk";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = {
    self,
    nixpkgs,
    flake-utils,
    naersk,
    fenix,
  }:
    flake-utils.lib.eachDefaultSystem (
      system: let
        pkgs = (import nixpkgs) {
          inherit system;
        };

        toolchain = with fenix.packages.${system};
          fromToolchainFile {
            file = ./rust-toolchain.toml; # alternatively, dir = ./.;
            sha256 = "sha256-SDu4snEWjuZU475PERvu+iO50Mi39KVjqCeJeNvpguU=";
          };
      in rec {
        defaultPackage =
          (naersk.lib.${system}.override {
            # For `nix build` & `nix run`:
            cargo = toolchain;
            rustc = toolchain;
          }).buildPackage {
            src = ./.;
          };

        # For `nix develop` or `direnv allow`:
        devShell = pkgs.mkShell {
          buildInputs = with pkgs; [
            pkg-config
            openssl
            ocl-icd
            toolchain
            rust-analyzer
            clang
          ];
          shellHook = ''
            export RUST_SRC_PATH="${toolchain}/lib/rustlib/src/rust/library"
            export PATH="${toolchain}/bin:$PATH"
            export RUSTUP_HOME=""
            export CARGO_HOME=""

            echo "Rust toolchain configured: $(rustc --version)"
          '';
        };
      }
    );
}
