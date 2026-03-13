# magcof

This directory keeps the World Magnetic Model assets used by the Rust CLI.

## Purpose

- Provide WMM coefficient data for magnetic declination calculations in navdata processing.
- Keep model data in-repo so the Rust CLI can run without external geomag downloads.

## Directory Layout

- `native/inibuilds_navdata_cli/src/core/magcof/`: bundled coefficient files (`WMM.COF`, `WMMHR.COF`).
- `native/inibuilds_navdata_cli/src/core/magnetic.rs`: active pure-Rust WMM implementation.

## Build

The active project path now uses direct Rust calls for geomag calculations. In normal development, building `inibuilds_navdata_cli` is sufficient.

## Notes

- Main project integration is in `native/inibuilds_navdata_cli/src/core/magnetic.rs`.
- Geomag C source/header files were removed after the Rust migration.
