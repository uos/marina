{
  description = "Marina";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = {
    flake-utils,
    nixpkgs,
    self,
    ...
  }:
    flake-utils.lib.eachDefaultSystem (
      system: let
        pkgs = import nixpkgs {
          inherit system;
        };

        docsBuildScripts = import ./nix/BuildDocs.nix {inherit pkgs;};

        marina = pkgs.rustPlatform.buildRustPackage {
          pname = "marina";
          version = "0.2.7";
          src = ./.;
          cargoLock.lockFile = ./Cargo.lock;
          nativeBuildInputs = [pkgs.pkg-config pkgs.installShellFiles];
          doCheck = false;
          postInstall = ''
            export MARINA_CONFIG_DIR=$(mktemp -d)
            export MARINA_CACHE_DIR=$(mktemp -d)
            installShellCompletion --cmd marina \
              --bash <($out/bin/marina completions bash) \
              --zsh <($out/bin/marina completions zsh) \
              --fish <($out/bin/marina completions fish)
          '';
        };
      in {
        packages = {
          marina = marina;
          default = marina;
        };

        apps = {
          buildDocs = {
            type = "app";
            program = "${docsBuildScripts.build}/bin/${docsBuildScripts.build.name}";
          };

          serveDocs = {
            type = "app";
            program = "${docsBuildScripts.serve}/bin/${docsBuildScripts.serve.name}";
          };
        };
      }
    );
}

