{
  description = "Godon controller - lifecycle verbs and the data layer";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-25.11";
    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay.url = "github:oxalica/rust-overlay";
  };

  outputs = { self, nixpkgs, flake-utils, rust-overlay }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs { inherit system overlays; };
        rustToolchain = pkgs.pkgsBuildHost.rust-bin.stable.latest.default;

        godon-controller = { version ? (if builtins.getEnv "GODON_VERSION" == "" then
          "DEV_BUILD"
        else
          builtins.getEnv "GODON_VERSION") }:
          pkgs.stdenv.mkDerivation {
            pname = "godon-controller";
            inherit version;
            src = ./.;

            nativeBuildInputs = with pkgs; [ rustToolchain ];

            env = {
              SSL_CERT_FILE = "${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt";
              NIX_SSL_CERT_FILE = "${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt";
              CURL_CA_BUNDLE = "${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt";
            };

            buildPhase = ''
              echo "Building godon-controller version: ${version}"
              export CARGO_HOME=$PWD/.cargo-home
              cargo build --release
            '';

            installPhase = ''
              mkdir -p $out/bin
              cp target/release/godon-controller $out/bin/godon-controller
              chmod +x $out/bin/godon-controller
            '';

            meta = with pkgs.lib; {
              description = "The godon controller - lifecycle verbs and the data layer";
              license = licenses.agpl3Only;
              platforms = platforms.linux;
            };
          };

      in {
        packages.default = godon-controller { };
        packages.godon-controller = godon-controller;

        devShells.default = pkgs.mkShell {
          buildInputs = with pkgs; [ rustToolchain rust-analyzer cargo-watch ];

          shellHook = ''
            echo "Godon controller development environment"
            echo "Rust: $(rustc --version)"
          '';
        };
      });
}
