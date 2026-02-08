Forza Car Editor
================

Overview
--------

**Forza Constructor Studio** is a Windows tool for editing and extending car data in _Forza Motorsport 4_ (Xbox 360).

It allows you to safely inspect, clone, and modify cars, engines, and upgrade data stored in the game’s SLT (SQLite) databases, while respecting how the game expects runtime data to be structured.

The tool supports working across the MAIN SLT and any number of DLC SLTs, while **only ever writing to MAIN**.

You can use it to:

*   Load the MAIN SLT and any number of DLC SLTs
    
*   Clone cars safely into MAIN
    
*   Clone and assign engines safely
    
*   Edit car, body, engine, drivetrain, and upgrade data
    
*   Create and edit engines
    
*   Add, duplicate, and customize upgrade levels (including beyond stock/race)
    
*   Work across MAIN + DLC data without corrupting runtime dependencies
    

This tool is designed to be **explicit, modular, and transparent**.Nothing is modified unless you explicitly apply a change.

### A word of warning

**I cannot code.**Almost all of this tool was created using ChatGPT, based on my understanding of how Forza Motorsport 4’s SLT files work and how the data is connected internally.

Because of that:

*   Always back up your files
    
*   Expect rough edges
    
*   Treat this as a research and modding tool, not a polished product
    

**⚠️ Offline modding / research use only.**I cannot take responsibility for lost or corrupted data.

General workflow
----------------

1.  Select MAIN SLT
    
2.  (Optional) Select DLC folder
    
3.  Reload sources
    
4.  Select a car from the left list
    
5.  Use tabs on the right to clone or edit
    
6.  Apply changes (always writes only to MAIN)
    

### MAIN vs DLC behavior (important)

*   **MAIN SLT is the only file ever modified**
    
*   **DLC SLTs are read-only**
    
*   DLC cars and engines can be selected as donors
    
*   If something required by a clone does not exist in MAIN, the tool will clone it automatically when needed
    

This mirrors how Forza expects data to exist at runtime and avoids crashes caused by missing dependencies.

Top bar buttons
---------------

### Select MAIN SLT

Choose the main database file. This is required before doing anything else.

### Select DLC Folder (optional)

Choose a folder containing DLC SLTs.The tool scans recursively, so subfolders are supported.

### Reload sources

Rebuilds the internal list of SLTs (MAIN + DLC).Use this whenever you change files on disk.

### Build lookup cache

Builds human-readable lookup tables (EnginePlacement, Materials, Cylinders, etc).Required for dropdowns to populate correctly.

### Backup MAIN now

Creates a timestamped backup of the MAIN SLT.

### Validate selected car (basic)

Runs basic sanity checks on the selected car’s references.

Left panel: Car list
--------------------

Shows all cars found across MAIN and DLC SLTs.

**Columns**

*   CarID
    
*   MediaName (more readable than string IDs)
    
*   Year
    
*   Source (which SLT the car comes from)
    

**Search box**Filters by CarID or MediaName.

**Show only cloned**Shows cars that are likely clones:

*   ModelYear = 6969
    
*   or CarID ≥ 2000
    

**Sort by**Sort by CarID, MediaName, Year, or Source.

Selecting a car sets the active target for all tabs.

Tab: Cloner
-----------

### Purpose

Clone a donor car into MAIN using a proven and safe cloning workflow.

### Controls

**Backup MAIN before cloning**Recommended. Creates a backup automatically.

**New CarID**CarID for the cloned car.By convention, cloned cars should start at **2000 or higher**.

**Suggest next**Finds the highest existing CarID across MAIN + DLC and suggests the next safe value.

**Year marker**Sets Data\_Car.ModelYear for the clone (default: 6969).Used only for identification.

**Clone selected donor into MAIN**Clones the currently selected car.

If the donor is from DLC, MAIN is also scanned for related rows.

### What gets cloned

*   Data\_Car
    
*   Data\_CarBody
    
*   Required stock upgrade rows only
    
*   Upgrade physics and references with safe re-keying
    
*   Combo and dependency tables only when required
    

This avoids duplicated globals and runtime crashes.

Tab: Car (Data\_Car)
--------------------

### Purpose

Edit core car parameters.

Editable fields include:

*   CarTypeID (Production / Race / Pre-Tuned)
    
*   EnginePlacementID (dropdown)
    
*   MaterialTypeID (dropdown)
    
*   Weight and weight distribution
    
*   Gear count
    
*   Tire sizes and wheel diameters
    
*   Base cost
    
*   Unicorn flag
    

**Wheel diameter safety**Front and rear wheel diameters are clamped between **13 and 24**.

**Save changes to MAIN**Writes changes only to MAIN.

**Reload from selected source**Reloads original values from the source SLT.

Tab: Body (Data\_CarBody)
-------------------------

### Purpose

Edit physical body parameters.

Editable fields:

*   Wheelbase
    
*   Track width
    
*   Ride height (front and rear)
    

Notes:

*   Cloned cars always use CarIDs ≥ 2000
    
*   Default clone year marker is 6969
    
*   Car names cannot be changed here (string tables are out of scope)
    

Tab: Engine (Lab + Assign)
--------------------------

### Purpose

Manage engines and engine assignment.

**Left side**List of all engines across MAIN and DLC.

**Search box**Filter by EngineName or MediaName.

### Buttons

**Assign selected engine as STOCK**Assigns the selected engine as the stock engine for the active car.

**Clone selected engine into MAIN + Assign**Clones the engine into MAIN if needed and assigns it safely.

### Engine editor (MAIN only)

Once an engine exists in MAIN, it can be edited.

Editable fields include:

*   Mass
    
*   MediaName (dropdown)
    
*   Engine configuration (dropdown)
    
*   Cylinders (dropdown)
    
*   Compression
    
*   Variable timing (dropdown)
    
*   Boost
    
*   Torque and power graph limits
    
*   Engine name
    
*   Rotation
    
*   Carbureted / Diesel / Rotary flags
    

**Important**Engine cloning also clones all required TorqueCurve entries.Missing torque curves will cause crashes during race load.

Tab: Upgrade Editor
-------------------

### Purpose

Edit List\_Upgrade\* tables directly.

This is an advanced feature.

**Table dropdown**Shows all List\_Upgrade\* tables with shortened names(example: EngineCamshaft instead of List\_UpgradeEngineCamshaft)

**Load rows**Loads rows relevant to the selected car.

The tool automatically detects scope:

*   Car-scoped (Ordinal / CarID)
    
*   Engine-scoped (EngineID)
    
*   CarBody-scoped (CarBodyID)
    
*   Drivetrain-scoped
    

**Row editor**

*   Edit all fields
    
*   Dropdowns used where lookups exist
    
*   Add row (copy selected)
    
*   Delete row
    
*   Apply edits to MAIN
    

Supports custom upgrade levels beyond the game’s default 0–3.

Tab: Constructor (experimental)
-------------------------------

### Purpose

Apply subsystems from donor cars.

Example:

*   Engine from car A
    
*   Suspension from car B
    
*   Drivetrain from car C
    

Uses safe cloning and base-block remapping.This tab is still under heavy testing.

Safety notes
------------

*   Always back up MAIN before major changes
    
*   Avoid manual edits to global Combo\_\* tables
    
*   Never assign multiple stock engines or drivetrains to a car
    
*   Missing TorqueCurves or mismatched Combo tables will crash the game
    

Build notes
-----------

*   Windows only
    
*   Python 3.10+
    
*   PyInstaller recommended
    

If distributing builds, distribute the EXE only.

Status
------

Under active development.

If something breaks:

*   Restore from backup
    
*   Reload sources
    
*   Rebuild lookup cache
    

### Disclaimer

This project is not affiliated with Turn 10 Studios or Microsoft.For educational and offline modding purposes only.
