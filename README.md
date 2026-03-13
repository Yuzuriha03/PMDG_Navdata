# ✈️ Inibuilds NavData

[![Navigraph](https://img.shields.io/badge/Navigraph-Compatible-blue.svg)](https://navigraph.com/) 
[![MSFS 2020](https://img.shields.io/badge/MSFS%202020-Compatible-brightgreen.svg)](https://www.flightsimulator.com/) 
[![MSFS 2024](https://img.shields.io/badge/MSFS%202024-Compatible-brightgreen.svg)](https://www.flightsimulator.com/) 
[![iniBuilds A340](https://img.shields.io/badge/iniBuilds-A340-orange.svg)](https://www.inibuilds.com/) 
[![iniBuilds A350](https://img.shields.io/badge/iniBuilds-A350-orange.svg)](https://www.inibuilds.com/) 
[![License: MIT](https://img.shields.io/badge/License-MIT-lightgrey.svg)](LICENSE)

> Keep your Inibuilds Airbus A340/A350 equipped with the latest navigation data for Microsoft Flight Simulator 2020 & 2024

## 📑 Table of Contents

- [✨ Key Features](#-key-features)
- [🧭 Why Update Your NavData?](#-why-update-your-navdata)
- [📊 What's Included](#-whats-included)
- [✈️ Supported Aircraft](#️-supported-aircraft)
- [📥 Installation Guide](#-installation-guide)
- [🔧 Troubleshooting](#-troubleshooting)
- [⚠️ Disclaimer](#️-disclaimer)

## ✨ Key Features

- **Realistic Flight Planning** — Create routes with up-to-date waypoints, airways, and navigational aids
- **Accurate Navigation** — Fly with current frequencies, runways, SIDs, STARs, and approaches
- **Cross-Version Support** — Works with both MSFS 2020 and MSFS 2024
- **Native Rust CLI** — Ships as `pmdg_navdata_cli.exe` for converting XP12 navigation data to PMDG format
- **Simple Installation** — Clear step-by-step instructions for updating your aircraft

## 🧭 Why Update Your NavData?

Real-world navigation data changes with every 28-day AIRAC cycle. Keeping your A340/A350's navigation database current ensures:

- **Accuracy** — Avoid invalid routes, outdated procedures, and unrealistic experiences
- **Realism** — Fly with the same data professional pilots use in real-world operations
- **Compatibility** — Essential for online networks like VATSIM and IVAO
- **Safety** — Practice with current procedures to build proper habits

## 📊 What's Included

| File | Description |
|------|-------------|
| `vhfs` | VHF radio frequencies (ATC, VORs) |
| `terminal_waypoints` | Terminal area waypoints for arrivals/departures |
| `runways` | Runway information (identifiers, lengths, surfaces) |
| `enroute_waypoints` | En-route navigation waypoints (cruise phase) |
| `ndbs` | Non-Directional Beacon (NDB) data |
| `airways` | Airways for IFR flight planning |
| `airports` | Airport identification data |
| `iaps` | Instrument Approach Procedures (ILS, RNAV, VOR, etc.) |
| `sids` | Standard Instrument Departures |
| `stars` | Standard Terminal Arrival Routes |
| `pmdg_navdata_cli.exe` | Native Rust CLI for converting navdata to PMDG format |

## ✈️ Supported Aircraft

- Inibuilds Airbus A340-300
- Inibuilds Airbus A350-900
- Inibuilds Airbus A350-900ULR
- Inibuilds Airbus A350-1000

## 📥 Installation Guide

### 1️⃣ Obtain Updated NavData

You'll need a subscription from Navigraph:
- **[Navigraph](https://navigraph.com/)**

### 2️⃣ Convert NDB Data

- Download the packaged Rust CLI release
- Rename `config.example.ini` to `config.ini`, place it next to `pmdg_navdata_cli.exe`, and fill in your local paths
- Run `pmdg_navdata_cli.exe`
- Optional: use `pmdg_navdata_cli.exe --help` to inspect available flags such as `--config`, `--step`, `--timeout`, and `--skip-postprocess`

Common examples:

```powershell
# Show all available arguments
.\pmdg_navdata_cli.exe --help

# Use a specific config file path
.\pmdg_navdata_cli.exe --config .\config.ini

# Run only one processing step
.\pmdg_navdata_cli.exe --step airports

# Skip the final index-drop and VACUUM post-process stage
.\pmdg_navdata_cli.exe --skip-postprocess

# Combine arguments
.\pmdg_navdata_cli.exe --config .\config.ini --step airways --timeout 60 --skip-postprocess
```

### 3️⃣ Build From Source

If you want to compile the Rust CLI yourself:

1. Install the Rust toolchain from [rustup.rs](https://rustup.rs/)
2. Open a terminal in the repository root
3. Build the CLI:

```powershell
cd .\native\pmdg_navdata_cli
cargo build --release --bin pmdg_navdata_cli
```

The compiled executable will be generated at:

```powershell
.\native\pmdg_navdata_cli\target\release\pmdg_navdata_cli.exe
```

To run the compiled executable manually, place `config.ini` next to it and start it from that directory.

### 4️⃣ Clear Navigation Cache

> ⚠️ **CRITICAL STEP!** Delete the contents of the NavigationData folder (not the folder itself)

Find your appropriate path (*** refers to a340 or a350):

**MSFS 2020 (Microsoft Store)**
```
%LocalAppdata%\Packages\Microsoft.FlightSimulator_8wekyb3d8bbwe\LocalState\packages\inibuilds-aircraft-***\work\NavigationData
```

**MSFS 2020 (Steam)**
```
%APPDATA%\Microsoft Flight Simulator\Packages\inibuilds-aircraft-***\work\NavigationData
```

**MSFS 2024 (Microsoft Store)**
```
%LocalAppdata%\Packages\Microsoft.Limitless_8wekyb3d8bbwe\LocalState\WASM\MSFS2024\inibuilds-aircraft-***\work\NavigationData
```

**MSFS 2024 (Steam)**
```
%APPDATA%\Microsoft Flight Simulator 2024\WASM\MSFS2024\inibuilds-aircraft-***\work\NavigationData
```

### 5️⃣ Install New NavData

- Copy all converted and updated navigation files into the empty NavigationData folder

### 6️⃣ Restart MSFS

- Launch (or restart) Microsoft Flight Simulator
- Your A340/A350 will now use the updated navigation data

## 🔧 Troubleshooting

<details>
<summary><b>Always Create a Backup First</b></summary>
Before making any changes, copy your original NavigationData folder to a safe location. If anything goes wrong, you can restore this backup.
</details>

<details>
<summary><b>Verify Correct Paths</b></summary>
Double-check that you're working in the correct NavigationData folder for your specific MSFS version and distribution platform (MS Store vs. Steam).
</details>

<details>
<summary><b>Navigation Data Not Loading</b></summary>

- ✅ Verify you deleted the contents of NavigationData folder (not the folder itself)
- ✅ Confirm you copied all required files to the NavigationData folder
- ✅ Ensure you have proper file permissions
- ✅ Try completely exiting and restarting MSFS
</details>

<details>
<summary><b>Converter Issues</b></summary>
The `pmdg_navdata_cli.exe` tool must be used with a valid `config.ini` placed next to the executable. Start from `config.example.ini`, then check the input/output paths carefully.
</details>

<details>
<summary><b>Missing Procedures or Waypoints</b></summary>
If specific procedures or waypoints are missing, your navigation data might be incomplete or incorrectly converted. Try obtaining fresh data and repeating the process.
</details>

## ⚠️ Disclaimer

This repository is not officially affiliated with Inibuilds & Navigraph. Use this information and the provided tools at your own risk. Always back up your data before making changes.

**Happy Flying!** 🛫
