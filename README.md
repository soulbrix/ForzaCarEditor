Forza Car Editor
================

Overview
--------

**Forza Car Editor** is a Windows tool for editing and extending car data in _Forza Motorsport 4_ (Xbox 360).

It allows you to safely inspect, clone, and modify cars, engines, and upgrade data stored in the game’s SLT (SQLite) databases, while respecting how the game expects runtime data to be structured.

The tool supports working across the MAIN and any number of DLC databases, and also supports creating new databases

You can use it to:

*   Load the Main game database and any number of DLC databases (SLT files)
    
*   Clone cars safely into databases
    
*   Clone and assign engines safely
    
*   Edit car, body, engine, drivetrain, and upgrade data
    
*   Create and edit engines
    
*   Add, duplicate, and customize upgrade levels (including beyond stock/race)
    
*   Work across MAIN + DLC data without corrupting runtime dependencies
    

This tool is designed to be **explicit, modular, and transparent**. Nothing is modified unless you explicitly apply a change.

### A word of warning

**I cannot code.** Almost all of this tool was created using ChatGPT, based on my understanding of how Forza Motorsport 4’s SLT files work and how the data is connected internally.

Because of that:

 *   Always back up your files
    
 *   Expect rough edges
    
 *   Treat this as a research and modding tool, not a polished product
 
 *   Some UI is hidden due to spacing issues - move pane width bars around to show all available UI. 

    

**⚠️ Offline modding / research use only.** I cannot take responsibility for lost or corrupted data.

General workflow
----------------

1.  Select MAIN DB
    
2.  (Optional) Select DLC folder
    
3.  Reload sources
    
4.  Select a car from the left list
    
5.  Use tabs on the right to clone or edit
    
6.  Apply changes
 

Top bar buttons
---------------

### Select MAIN DB

Choose the main database file. This is required before doing anything else. Generally this would be the gamedb.slt file, but it can be any SLT file. However, in order for some functionality to work, you will need to provide the location of the gamedb.slt using the **Pick Lookup DB** button.

### Select DLC Folder (optional)

Choose a folder containing DLC SLTs. The tool scans recursively, so subfolders are supported.

### Reload sources

Rebuilds the internal list of SLTs (MAIN + DLC). Use this whenever you change files.

### Build lookup cache

Builds human-readable lookup tables (EnginePlacement, Materials, Cylinders, etc). Required for dropdowns to populate correctly.

### Backup Database

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
    

**Search box** 
Filters by CarID or MediaName.

**Show only cloned** 
Shows cars that are likely clones:

*   ModelYear = 6969
    
*   or CarID ≥ 2000
    

**Sort by**
Sort by CarID, MediaName, Year, or Source.

Selecting a car sets the active target for all tabs.

Tab: Cloner
-----------

### Purpose

Clone cars into the main database or a specific target database. You can also create new SLT files to be used as DLC (these need to be included in DLC folder structures, which is not covered in this readme).

DB files are backed up automatically in the App Root every time an edit is performed. 

### Controls

**Backup MAIN before cloning**
Recommended. Creates a backup automatically.

**Year marker**
Sets Data\_Car.ModelYear for the clone (default: 6969). Used only for identification.

**Template database**
Selects the database to be used as template - I've provided a file to be used here in the release. 

**Output Database**
Sets location and name of the target database file. **Needs to end in _merge to be used as DLC.**

**Create new database + clone selected car**
Creates file with the cloned car.

**Clone selected car into Main Database**
Clones car into main database.

**Target Database**
Selects specific database file where selected car will be cloned to.

**Clone car into selected Target database**
Clones car into selected database.

**Cloning options**

 - Also clone donor stock engine: clones engine and car together.
 - Reassign Drivetrain IDs: creates new Drivetrains to be used with the cloned car, instead of using references to existing drivetrains.
 - Clone only stock drivetrain info: only clones the stock level drivetrain entries.
 - Force CarID: specify a specific CarID if Auto Assign is not working. **Always use Auto Assign first!**

### What gets cloned

*   Data\_Car
    
*   Data\_CarBody
    
*   Upgrade parts
    
*   Upgrade physics and references with safe re-keying
    
*   Combo and dependency tables only when required
    

This avoids duplicated globals and runtime crashes.

Notes:

*   Cloned cars should always use CarIDs ≥ 2000
    
*   Default clone year marker is 6969 for filtering purpose, but can be changed.
    
*   Car names cannot be changed here (string tables are out of scope)

Tab: General Car Info
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
    

**Wheel diameter safety**
Front and rear wheel diameters are clamped between **13 and 24**.

**Apply to DB**
Writes changes.

**Load from DB**
Reloads original values from database.

Tab: Car Body
-------------------------

### Purpose

Edit physical body parameters.

Editable fields:

*   Wheelbase
    
*   Track width
    
*   Ride height (front and rear)
   
    

Tab: Engine lab
--------------------------

### Purpose

Manage engines and engine assignment.

***Move the width pane around to see all options!**

**Left side**
List of all engines across MAIN and DLC.

**Search box**
Filter by EngineName or MediaName.

### Buttons

**Assign to selected car**
Assigns the selected engine as the stock engine for the active car.

**Clone selected engine**
Clones the engine into MAIN DB.

**Target SLT**
Selects a specific DB file to be used.

**Clone into target**
Clones the engine into the selected target DB.

**Auto Assign**
Assigns an EngineID to be used. Choose a target SLT first, or check the highest EngineID in the list to avoid conflicts.

### Engine editor

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
    

**Important**
Engine cloning also clones all required TorqueCurve entries. Missing torque curves will cause crashes during race load.

Tab: Upgrade and Misc Editor
-------------------

### Purpose

Edit List\_Upgrade\* tables directly.

This is an advanced feature.

**Table dropdown**
Shows all List\_Upgrade\* tables with shortened names (example: EngineCamshaft instead of List\_UpgradeEngineCamshaft)

**Load rows**
Loads rows relevant to the selected car.

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
    

Supports custom upgrade levels beyond the game’s default 0–3 (Stock, Sports, Semi-Professional, Professional).

Tab: Spec Sheet
-------------------------------

### Purpose
Provides general read-only information about the selected car.

Tab: Diff Viewer
-------------------------------

### Purpose
Allows to compare differences between cars.


Tab: Engine Diff
-------------------------------

### Purpose
Allows to compare differences between engines.


Tab: Constructor (experimental)
-------------------------------

### Purpose

Apply subsystems from donor cars.

Example:

*   Engine from car A
    
*   Suspension from car B
    
*   Drivetrain from car C
    

Uses safe cloning and base-block remapping. This tab is still being tested and may not work.

Safety notes
------------

*   Always back up the DB files before major changes
    
*   Avoid manual edits to global Combo\_\* tables
    
*   Never assign multiple stock engines to a car
    
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

This project is not affiliated with Turn 10 Studios or Microsoft. For educational and offline modding purposes only.
