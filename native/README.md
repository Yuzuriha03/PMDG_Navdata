
# Native Code

This folder contains the project's native implementation layers.

## Layout

- `native/pmdg_navdata_cli`: the Rust CLI that converts navdata into the PMDG output format.
- `native/compare_databases`: a standalone Rust tool for SQLite database diffs used during validation and regression checks.
- `native/pmdg_navdata_cli/src/core/magcof`: bundled World Magnetic Model coefficient data consumed by the Rust CLI.

## Rust CLI

- Crate path: `native/pmdg_navdata_cli`
- Crate name: `pmdg_navdata_cli`
- Binary name: `pmdg_navdata_cli`

## Database Compare Tool for Development

- Crate path: `native/compare_databases`
- Crate name: `compare_databases`
- Binary name: `compare_databases`

### Source Layout

- `src/core`: shared utilities such as parsers, magnetic math, database helpers, and matchers
- `src/enroute`: enroute processing modules such as airways, VHF, NDB, GS, and enroute waypoints
- `src/airports`: airport and runway processing modules
- `src/terminal`: terminal CIFP procedure processing plus terminal waypoints

### Build

```powershell
cd native/pmdg_navdata_cli
cargo build --release --bin pmdg_navdata_cli
```

To build the database compare tool:

```powershell
cd native/compare_databases
cargo build --release
```
