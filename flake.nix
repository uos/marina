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
        docsBuildScripts = import ./nix/BuildDocs.nix {inherit pkgs;};
      in {
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

