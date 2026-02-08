PROJECT STATE SNAPSHOTForza Constructor Studio / Forza Car EditorStatus: **Race-stable car + engine cloning achieved**

ARCHITECTURE DECISIONS

*   Language: Python
    
*   UI: Tkinter (ttk)
    
*   Database format: SQLite (.slt)
    
*   Platform: Windows only
    
*   Game target: Forza Motorsport 4 (Xbox 360)
    

Core principle:

*   **MAIN SLT is the only writable database**
    
*   DLC SLTs are strictly read-only and used only as data sources (donors)
    

Design philosophy:

*   Explicit, conservative cloning
    
*   No hidden automation
    
*   No global table modification unless absolutely required for runtime stability
    
*   Prefer dependency-driven cloning over ID pattern assumptions
    

Major architectural split:

*   Car cloning logic (stable, proven)
    
*   Engine cloning logic (stable only after TorqueCurve dependency fix)
    
*   Constructor/editor logic (UI + controlled edits)
    
*   Upgrade editor (direct List\_Upgrade\* manipulation)
    

FILE STRUCTURE

Current core files:

*   constructor\_app.pyMain Tkinter UI applicationTabs:
    
    *   Cloner
        
    *   Car editor
        
    *   Body editor
        
    *   Engine Lab
        
    *   Upgrade Editor
        
    *   Constructor (experimental)
        
*   cloner\_engine.pyBackend logic for:
    
    *   Car cloning
        
    *   Engine cloning
        
    *   Dependency resolution
        
    *   TorqueCurve cloning
        
    *   Safe ID suggestion
        
*   constructor\_engine.pyBackend logic for:
    
    *   Loading cars/engines from MAIN + DLC
        
    *   Lookup cache building
        
    *   Editing Data\_\* and List\_\* tables
        
    *   Scope detection for upgrade tables
        

No personal data is embedded in any file.

FEATURE LIST (CURRENTLY WORKING)

GLOBAL

*   Load MAIN SLT
    
*   Load DLC folder (recursive)
    
*   Reload sources
    
*   Build lookup cache
    
*   Backup MAIN SLT
    
*   Read MAIN + DLC simultaneously
    
*   Write ONLY to MAIN
    

CAR LIST

*   Shows cars from MAIN + DLC
    
*   Columns: CarID, MediaName, Year, Source
    
*   Search/filter
    
*   Sort
    
*   “Show only cloned” (CarID ≥ 2000 or Year = 6969)
    

CAR CLONER (STABLE)

*   Clone car from MAIN or DLC into MAIN
    
*   Safe CarID suggestion (scans all sources)
    
*   Year marker (default 6969)
    
*   Correct cloning of:
    
    *   Data\_Car
        
    *   Data\_CarBody
        
    *   Required stock upgrade rows
        
    *   Physics tables
        
    *   ContentOffersMapping
        
*   Special handling:
    
    *   List\_UpgradeCarBody → ONLY stock row
        
*   No global Combo\_\* duplication
    

ENGINE LAB (STABLE AFTER FIX)

*   List engines from MAIN + DLC
    
*   Search by EngineName / MediaName
    
*   Assign engine as STOCK
    
*   Clone engine into MAIN + assign
    
*   Edit engine fields in MAIN
    

ENGINE CLONING (CRITICAL FIX COMPLETED)

*   Clones Data\_Engine
    
*   Clones List\_UpgradeEngine\* (excluding List\_UpgradeEngine itself)
    
*   **Clones all referenced TorqueCurveIDs**
    
    *   Dependency-driven (no fixed ID math)
        
    *   Prevents race-load crashes
        
*   Combo\_Engines correctly synced on assign
    

UPGRADE EDITOR

*   Edit any List\_Upgrade\* table
    
*   Automatic scope detection:
    
    *   Car (Ordinal / CarID)
        
    *   Engine (EngineID)
        
    *   CarBody
        
    *   Drivetrain
        
*   Dropdowns with human-readable names when lookups exist
    
*   Supports upgrade levels beyond 0–3
    

CONSTRUCTOR TAB (EXPERIMENTAL)

*   Mix subsystems from different donor cars
    
*   Uses safe cloning rules
    
*   Not fully validated in game
    

IMPORTANT PAST DECISIONS

*   CarID and EngineID for clones start at **2000+**
    
*   Year marker **6969** used to identify clones
    
*   Never modify DLC SLTs
    
*   Never blindly clone Combo\_\* tables
    
*   Never assume ID patterns (e.g. EngineID\*1000) for dependencies
    
*   **TorqueCurve cloning is mandatory for engine clones**
    
*   Combo\_Engines must match stock engine or race load will crash
    
*   List\_UpgradeEngine must have exactly ONE stock row per car
    

NAMING CONVENTIONS

*   Cloned CarIDs: ≥ 2000
    
*   Cloned EngineIDs: ≥ 2000
    
*   Base block math:
    
    *   CarBody IDs: CarID \* 1000 + offset
        
    *   Color combos: CarID \* 1000 + offset
        
*   ModelYear = 6969 for cloned cars (identifier only)
    

CONSTRAINTS

*   Tkinter UI (no web UI)
    
*   Python only
    
*   Windows only
    
*   Offline modding / research use only
    
*   No string (.str) editing in scope
    
*   Must respect FM4 runtime expectations (race load is the real test)
    

CURRENT BUGS / LIMITATIONS

*   Constructor tab is experimental and not fully validated
    
*   No automated deep “delete clone” yet
    
*   No formal validation report UI (only basic checks)
    
*   No undo system (backups are required)
    
*   Engine assignment bugs were fixed, but editor misuse can still cause invalid states if used incorrectly
    
*   No guardrails against assigning incompatible engines/drivetrains
    

UNFINISHED / FUTURE TASKS

HIGH PRIORITY

*   Engine validation tool:
    
    *   Detect missing TorqueCurveIDs
        
    *   Detect mismatched Combo\_Engines
        
*   Car validation tool:
    
    *   Check required dependencies exist
        
*   Safe “delete cloned car/engine” feature
    
*   Freeze current version as v1.0-race-stable
    

MEDIUM PRIORITY

*   Improve Constructor tab stability
    
*   Add warnings before dangerous edits
    
*   Add read-only diff preview before apply
    

LOW PRIORITY / NICE TO HAVE

*   Preset system
    
*   Dark UI theme
    
*   Export validation reports
    
*   Advanced dependency visualization
    

NEXT STEPS (RECOMMENDED)

1.  **Freeze current working version**
    
2.  Create a clean MAIN SLT backup
    
3.  Add validation buttons (engine + car)
    
4.  Add deep delete for clones
    
5.  Only then consider new features
    

SUMMARY

This project has reached a major milestone:

*   Fully working cloned cars
    
*   Fully working cloned engines
    
*   Race-stable runtime behavior
    

The hardest FM4 problems (TorqueCurves, Combo tables, upgrade scoping) are solved.

Further work should prioritize **stability, validation, and guardrails**, not new power features.
