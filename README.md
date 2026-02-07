# Forza Constructor Studio

## Overview

Forza Constructor Studio is a Windows tool for editing and extending car data in Forza Motorsport 4 (Xbox 360).
It allows you to:

- Load the MAIN SLT and any number of DLC SLTs
- Clone cars safely into MAIN
- Edit car, body, engine, drivetrain, and upgrade data
- Create and edit engines
- Add, duplicate, and customize upgrade levels (including beyond stock/race)
- Work across MAIN + DLC data while always writing only to MAIN

This tool is designed to be safe, modular, and transparent.
Nothing is hidden, and no changes are made unless you explicitly apply them.

### A word of warning
**I cannot code, so almost all of this app was created using ChatGPT.**
I simply fed it my knowledge of how the SLT file works and the connections between the cars.

**⚠️ Offline modding / research use only.**
Always back up your original files. I cannot take responsibility for lost or corrupted data.

## General workflow

1. Select MAIN SLT
2. (Optional) Select DLC folder
3. Reload sources
4. Select a car from the left list
5. Use tabs on the right to clone or edit
6. Save/apply changes (always writes only to MAIN)

### MAIN vs DLC behavior (important)

MAIN SLT is the only file ever modified
DLC SLTs are read-only
DLC cars and engines can be selected as donors
If something does not exist in MAIN, the tool will clone it when required
This avoids corruption and matches how Forza expects data to exist at runtime.

### Top bar buttons

- Select MAIN SLT
Choose the main database file. This is required before doing anything else.

- Select DLC Folder (optional)
Choose a folder that contains DLC SLTs.
The tool scans recursively, so subfolders are supported.

- Reload sources
Rebuilds the internal list of SLTs (MAIN + DLC).
Use this whenever you change files on disk.

- Build lookup cache
Builds human-readable name lookups (EnginePlacement, Materials, Cylinders, etc).
Required for dropdowns to populate correctly.

- Backup MAIN now
Creates a timestamped backup of the MAIN SLT.

- Validate selected car (basic)
Runs basic sanity checks on the selected car’s references.

### Left panel: Car list

This list shows all cars found across MAIN and DLC SLTs.

- Columns:
  - CarID
  - MediaName (more readable than string IDs)
  - Year
  - Source (which SLT it comes from)

- Search box
Filters by CarID or MediaName.

- Show only cloned
Shows cars that are likely clones:
  - ModelYear = 6969
  - or CarID >= 2000

- Sort by
Sort the list by CarID, MediaName, Year, or Source.

- Selecting a car
Selecting a car here sets the active target for all tabs.

### Tab: Cloner

- Purpose
Clone a donor car into MAIN.
This uses the proven Car Cloner logic and does not interfere with Constructor editing.

Controls

- Backup MAIN before cloning
Recommended. Creates a backup automatically.

- New CarID
The CarID for the cloned car.
By convention, cloned cars should start at 2000 or higher.

- Suggest next
Automatically finds the highest existing CarID and suggests the next safe value.

- Year marker
Sets Data_Car.Year for the cloned car (default: 6969).
This is purely for identification and does not affect gameplay.

- Clone selected donor into MAIN
Clones the currently selected car (from the left list).
If the donor is from DLC, MAIN is also scanned for extra related rows.

What gets cloned
  - Data_Car
  - Data_CarBody
  - Stock upgrade rows
  - Upgrade physics (with safe re-keying)
  - Required references only (to avoid crashes)

### Tab: Car (Data_Car)

- Purpose
Edit core car parameters.

Editable fields include:
- CarTypeID (Production / Race / Pre-Tuned)
- EnginePlacementID (dropdown)
- MaterialTypeID (dropdown)
- Weight and distribution
- Gear count
- Tire sizes and wheel diameters
- Base cost
- Unicorn flag

- Wheel diameter safety
Front/Rear wheel diameter values are clamped between 13 and 24.

- Save changes to MAIN
Writes changes to the MAIN SLT only.

- Reload from selected source
Reloads original values from the source SLT (MAIN or DLC).

### Tab: Body (Data_CarBody)

- Purpose
Edit physical body parameters.

Editable fields include:
  - Wheelbase
  - Track width
  - Ride height (front and rear)
  - Changes apply only to MAIN.

### Tab: Engine (Lab + Assign)

- Purpose
Manage engines and engine assignment.

- Left side
List of all engines across MAIN and DLC.

- Search box
Filters engines by name or MediaName.

- Assign selected engine as STOCK
Sets the selected engine as the stock engine for the current car.

- Clone selected engine into MAIN + Assign
If the engine is from DLC, it is cloned into MAIN and then assigned.

- Engine editor (MAIN only)
Once an engine exists in MAIN, it can be edited.

Editable engine fields include:
  - Mass
  - MediaName (dropdown)
  - Engine configuration (dropdown)
  - Cylinders (dropdown)
  - Compression
  - Variable timing (dropdown)
  - Boost
  - Torque and power graph limits
  - Engine name
  - Rotation
  - Carbureted / Diesel / Rotary flags

- Load engine fields from MAIN
Loads current values.

- Apply engine edits to MAIN
Writes changes to MAIN.

### Tab: Upgrade Editor

- Purpose
Edit List_Upgrade* tables directly.

This is an advanced feature and allows full control.

- Table dropdown
Shows all List_Upgrade* tables, with shortened names for readability
(example: EngineCamshaft instead of List_UpgradeEngineCamshaft)

- Load rows
Loads rows relevant to the selected car.

The tool automatically detects scope:
  - Car-scoped (Ordinal / CarID)
  - Engine-scoped (EngineID)
  - CarBody-scoped (CarBodyID)
  - Drivetrain-scoped (PowertrainID / DrivetrainID)

- Rows list
Shows Level and IsStock where applicable.

- Field editor
Edits all fields in the selected row.
Dropdowns are used when lookups exist.

Special handling:
  - TireCompoundID uses friendly names
  - EngineID shows all engines across SLTs
  - PowertrainID shows drivetrain type + mounting direction
  - Wheel diameter values are clamped
  - Add row (copy selected)
  - Duplicates the selected row and automatically assigns the correct scope value.

- Delete selected row
Removes the selected row from MAIN.

- Apply row edits
Writes the edited row to MAIN.

This tab supports custom upgrade levels beyond the game’s default 0–3.

### Tab: Constructor (UNTESTED)

- Purpose
Apply subsystems from donor cars (engine, suspension, drivetrain, etc).

This allows mixing components from different cars.

Example:
- Engine from car A
- Suspension from car B
- Drivetrain from car C

This tab uses safe cloning rules and base-block remapping.

## Safety notes

- Always back up MAIN before major changes
- Avoid editing global Combo_* tables
- Do not assign multiple stock engines or drivetrains to a car
- The tool enforces these rules where possible

## Build notes

- This is a Windows-only tool.

### Recommended build:

Python 3.10+
PyInstaller

If you distribute builds, distribute the EXE only.
The source contains no personal data or identifying information.

## Status

This tool is under active development.
If something breaks:
- Restore from backup
- Reload sources
- Rebuild lookup cache

### Disclaimer
This project is not affiliated with Turn 10 Studios or Microsoft. For educational and modding purposes only.
