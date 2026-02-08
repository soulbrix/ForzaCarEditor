# constructor_app.py
from __future__ import annotations

import os
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import traceback

from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

import constructor_engine as ce


# Defaults so UI callbacks never crash with NameError
clone_car_between = None
suggest_next_car_id = None
clone_engine_to_main = None
suggest_next_engine_id = None
cloner_import_error = None
try:
    import importlib
    import sys

    if "cloner_engine" in sys.modules:
        del sys.modules["cloner_engine"]

    import cloner_engine as cl
    importlib.reload(cl)

    clone_engine_to_main = getattr(cl, "clone_engine_to_main", None)
    suggest_next_engine_id = getattr(cl, "suggest_next_engine_id", None)
    clone_car_between = getattr(cl, "clone_car_between", None)
    suggest_next_car_id = getattr(cl, "suggest_next_car_id", None)

except Exception as e:
    cloner_import_error = traceback.format_exc()
    clone_engine_to_main = None
    suggest_next_engine_id = None

    print("==== CLONER ENGINE IMPORT FAILED ====")
    print(cloner_import_error)
    print("====================================")




APP_TITLE = f"Forza Constructor Studio ({ce.CONSTRUCTOR_VERSION})"


class ConstructorApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title(APP_TITLE)
        self.geometry("1200x720")
        self.minsize(1100, 650)

        self.main_db: Optional[Path] = None
        self.dlc_folder: Optional[Path] = None
        self.sources: List[Path] = []  # includes MAIN + DLC paths
        self.lookup_cache: Dict[str, Dict[int, str]] = {}

        self.selected_car_id: Optional[int] = None
        self.selected_car_source: Optional[Path] = None

        self.selected_engine_id: Optional[int] = None
        self.selected_engine_source: Optional[Path] = None

        self._build_ui()

    # ----------------------------
    # UI build
    # ----------------------------
    def _resolve_source_path(self, source_value: str) -> Optional[Path]:
        """
        source_value can be a full path OR just a filename.
        We resolve it against self.sources.
        """
        if not source_value:
            return None

        p = Path(source_value)
        if p.exists():
            return p

        # Try match by basename against loaded sources
        sv = source_value.lower()
        for sp in self.sources:
            if sp.name.lower() == sv:
                return sp

        return None

    def clone_selected_engine_into_main(self):
        if clone_engine_to_main is None:
            messagebox.showerror(
                "Cloner not available",
                cloner_import_error or "Unknown import error"
            )
            return
        if not self.main_db:
            messagebox.showerror("Missing MAIN", "Select MAIN SLT first.")
            return
        if self.selected_engine_id is None:
            messagebox.showerror("No engine selected", "Select an engine in the engine list first.")
            return

        # Resolve source path robustly
        if not self.selected_engine_source or not Path(self.selected_engine_source).exists():
            messagebox.showerror("Bad source", "Could not resolve engine source SLT path. Try Reload sources.")
            return

        src_path = Path(self.selected_engine_source)
        src_engine_id = int(self.selected_engine_id)

        try:
            new_engine_id = int(self.new_engine_id_var.get())
        except Exception:
            messagebox.showerror("Invalid EngineID", "New EngineID must be an integer.")
            return

        # Backup MAIN
        try:
            backup_path = ce.backup_db(self.main_db)
        except Exception as e:
            messagebox.showerror("Backup failed", str(e))
            return

        try:
            clone_engine_to_main(
                source_db=src_path,
                main_db=self.main_db,
                source_engine_id=src_engine_id,
                new_engine_id=new_engine_id,
            )
        except Exception as e:
            messagebox.showerror("Clone engine failed", str(e))
            return

        self._log(
            "Engine cloned ✅\n"
            f"Source: {src_path.name} — EngineID {src_engine_id}\n"
            f"New in MAIN: EngineID {new_engine_id}\n"
            f"Backup: {backup_path}\n\n"
        )
        messagebox.showinfo("Engine cloned", f"Cloned Engine {src_engine_id} → {new_engine_id} into MAIN.")

        self.refresh_engine_list()
        
    def _suggest_engine_id(self):
        if suggest_next_engine_id is None:
            msg = "suggest_next_engine_id not available (cloner_engine import failed)."
            if cloner_import_error:
                msg += f"\n\nImport error:\n{cloner_import_error}"
            messagebox.showerror("Suggest EngineID failed", msg)
            return

        if not self.main_db:
            messagebox.showerror("Missing MAIN", "Select MAIN SLT first.")
            return

        try:
            # Scan MAIN + DLC sources to find true highest EngineID, then suggest next
            next_id = suggest_next_engine_id(main_db=self.main_db, min_id=2000, aux_sources=self.sources)
            self.new_engine_id_var.set(int(next_id))
            self._log(f"Suggested next EngineID: {next_id}\n")
        except Exception as e:
            messagebox.showerror("Suggest EngineID failed", str(e))



    def _build_ui(self):
        top = ttk.Frame(self)
        top.pack(side="top", fill="x", padx=10, pady=8)

        ttk.Button(top, text="Select MAIN SLT", command=self.pick_main).pack(side="left")
        self.main_lbl = ttk.Label(top, text="MAIN: (none)")
        self.main_lbl.pack(side="left", padx=10)

        ttk.Button(top, text="Select DLC Folder (optional)", command=self.pick_dlc).pack(side="left", padx=6)
        self.dlc_lbl = ttk.Label(top, text="DLC: (none)")
        self.dlc_lbl.pack(side="left", padx=10)

        ttk.Button(top, text="Reload sources", command=self.reload_sources).pack(side="left", padx=6)
        ttk.Button(top, text="Build lookup cache", command=self.rebuild_cache).pack(side="left", padx=6)

        mid = ttk.PanedWindow(self, orient="horizontal")
        mid.pack(side="top", fill="both", expand=True, padx=10, pady=8)

        # Left: car list + search
        left = ttk.Frame(mid)
        mid.add(left, weight=1)

        sr = ttk.Frame(left)
        sr.pack(fill="x", pady=(0, 6))
        ttk.Label(sr, text="Car search:").pack(side="left")
        self.car_search_var = tk.StringVar()
        ent = ttk.Entry(sr, textvariable=self.car_search_var)
        ent.pack(side="left", fill="x", expand=True, padx=6)
        ent.bind("<KeyRelease>", lambda e: self.refresh_car_list())
        self.only_clones_var = tk.IntVar(value=0)
        ttk.Checkbutton(
            sr,
            text="Show only cloned (Year=6969 or CarID>=2000)",
            variable=self.only_clones_var,
            command=self.refresh_car_list,
        ).pack(side="left", padx=6)

        sortbar = ttk.Frame(left)
        sortbar.pack(fill="x", pady=(0, 6))
        ttk.Label(sortbar, text="Sort by:").pack(side="left")
        self.car_sort_var = tk.StringVar(value="CarID")
        ttk.Combobox(
            sortbar,
            textvariable=self.car_sort_var,
            state="readonly",
            values=["CarID", "MediaName", "Year", "Source"],
        ).pack(side="left", padx=6)
        ttk.Button(sortbar, text="Apply", command=self.refresh_car_list).pack(side="left")

        cols = ("CarID", "MediaName", "Year", "Source")
        self.car_tree = ttk.Treeview(left, columns=cols, show="headings", height=18)
        for c in cols:
            self.car_tree.heading(c, text=c)
            self.car_tree.column(c, width=140 if c != "MediaName" else 340, anchor="w")
        self.car_tree.pack(fill="both", expand=True)

        ys = ttk.Scrollbar(left, orient="vertical", command=self.car_tree.yview)
        self.car_tree.configure(yscrollcommand=ys.set)
        ys.place(in_=self.car_tree, relx=1.0, rely=0, relheight=1.0, anchor="ne")

        self.car_tree.bind("<<TreeviewSelect>>", self.on_car_select)

        # Right: tabs
        right = ttk.Frame(mid)
        mid.add(right, weight=2)

        self.nb = ttk.Notebook(right)
        self.nb.pack(fill="both", expand=True)

        # NEW: Cloner tab (kept separate from constructor workflow)
        self._build_tab_cloner()

        self._build_tab_car()
        self._build_tab_body()
        self._build_tab_engine()
        self._build_tab_upgrades()
        self._build_tab_constructor()

        bottom = ttk.Frame(self)
        bottom.pack(side="bottom", fill="x", padx=10, pady=8)

        ttk.Button(bottom, text="Backup MAIN now", command=self.backup_main).pack(side="left")
        ttk.Button(bottom, text="Validate selected car (basic)", command=self.validate_selected_car).pack(
            side="left", padx=6
        )

        self.log = tk.Text(bottom, height=5, wrap="word")
        self.log.pack(side="left", fill="both", expand=True, padx=10)
        self._log("Constructor Studio ready.\nSelect MAIN SLT first.\n")

    # ----------------------------
    # NEW TAB: Cloner
    # ----------------------------
    
    def _build_tab_cloner(self):
        tab = ttk.Frame(self.nb)
        self.nb.add(tab, text="Cloner")

        frm = ttk.Frame(tab)
        frm.pack(fill="both", expand=True, padx=10, pady=10)

        info = (
            "Clones the currently selected donor car (left list) into MAIN.\n"
            "If the donor is from a DLC SLT, the cloner will also look in MAIN for extra rows to copy."
        )
        ttk.Label(frm, text=info, justify="left").grid(row=0, column=0, columnspan=6, sticky="w", pady=(0, 10))

        self.cloner_backup_var = tk.IntVar(value=1)
        ttk.Checkbutton(frm, text="Backup MAIN before cloning", variable=self.cloner_backup_var).grid(
            row=1, column=0, columnspan=2, sticky="w"
        )

        ttk.Label(frm, text="New CarID:").grid(row=2, column=0, sticky="w", pady=6)
        self.cloner_new_id_var = tk.IntVar(value=2000)
        ttk.Entry(frm, textvariable=self.cloner_new_id_var, width=12).grid(row=2, column=1, sticky="w")
        ttk.Button(frm, text="Suggest next", command=self._cloner_suggest_next_id).grid(
            row=2, column=2, sticky="w", padx=8
        )

        ttk.Label(frm, text="Year marker:").grid(row=3, column=0, sticky="w", pady=6)
        self.cloner_year_var = tk.IntVar(value=6969)
        ttk.Entry(frm, textvariable=self.cloner_year_var, width=12).grid(row=3, column=1, sticky="w")
        ttk.Label(frm, text="(sets Data_Car.Year so you can spot clones)").grid(
            row=3, column=2, columnspan=3, sticky="w"
        )

        ttk.Separator(frm).grid(row=4, column=0, columnspan=6, sticky="ew", pady=12)

        ttk.Button(frm, text="Clone selected donor into MAIN", command=self.clone_selected_car_into_main).grid(
            row=5, column=0, columnspan=2, sticky="w"
        )

        self.cloner_status = ttk.Label(frm, text="")
        self.cloner_status.grid(row=6, column=0, columnspan=6, sticky="w", pady=(12, 0))

        frm.columnconfigure(5, weight=1)

    def _cloner_suggest_next_id(self):
        if suggest_next_car_id is None:
            messagebox.showerror(
                "Cloner not available",
                cloner_import_error or "Unknown import error"
            )
            return
        if not self.main_db:
            messagebox.showerror("Missing MAIN", "Select MAIN SLT first.")
            return
        try:
            next_id = suggest_next_car_id(self.main_db, min_id=2000)
            self.cloner_new_id_var.set(int(next_id))
            self._log(f"Suggested next CarID: {next_id}\n")
        except Exception as e:
            messagebox.showerror("Suggest failed", str(e))

    def clone_selected_car_into_main(self):
        if clone_car_between is None:
            messagebox.showerror(
                "Cloner not available",
                cloner_import_error or "Unknown import error"
            )
            return
        if not self.main_db:
            messagebox.showerror("Missing MAIN", "Select MAIN SLT first.")
            return
        if self.selected_car_id is None or self.selected_car_source is None:
            messagebox.showerror("No donor selected", "Select a donor car in the left list first.")
            return

        donor_id = int(self.selected_car_id)
        donor_src = Path(self.selected_car_source)

        try:
            new_id = int(self.cloner_new_id_var.get())
        except Exception:
            messagebox.showerror("Invalid CarID", "New CarID must be an integer.")
            return

        try:
            year_marker = int(self.cloner_year_var.get())
        except Exception:
            messagebox.showerror("Invalid year", "Year marker must be an integer.")
            return

        # Optional backup
        backup_path = None
        if int(self.cloner_backup_var.get()) == 1:
            try:
                backup_path = ce.backup_db(self.main_db)
            except Exception as e:
                messagebox.showerror("Backup failed", str(e))
                return

        # If donor is from DLC, also use MAIN as extra source (common: extra upgrade rows live in MAIN)
        extra = None
        try:
            if donor_src.resolve() != self.main_db.resolve():
                extra = self.main_db
        except Exception:
            extra = self.main_db

        try:
            rep = clone_car_between(
                source_db=donor_src,
                target_db=self.main_db,
                source_car_id=donor_id,
                new_car_id=new_id,
                year_marker=year_marker,
                extra_source_db=extra,
            )
        except Exception as e:
            messagebox.showerror("Clone failed", str(e))
            return

        self.cloner_status.configure(text=f"Cloned Car {donor_id} → {new_id} into MAIN.")
        self._log("Clone complete ✅\n")
        self._log(f"Donor: {donor_src.name} — CarID {donor_id}\n")
        self._log(f"New in MAIN: CarID {new_id} (Year={year_marker})\n")
        if backup_path:
            self._log(f"Backup: {backup_path}\n")
        self._log("Tables touched:\n")
        for t, n in rep.tables_touched.items():
            self._log(f"  {t}: {n}\n")
        self._log("\n")

        # Refresh UI
        self.refresh_car_list()

    def _build_tab_car(self):
        tab = ttk.Frame(self.nb)
        self.nb.add(tab, text="General Car Info")

        self.car_fields: Dict[str, tk.Variable] = {}

        # Field definitions: (column, label, widget_type)
        # widget_type: "num", "text", "bool", "dropdown"
        fields = [
            ("CarTypeID", "CarTypeID (1 Production, 2 Race, 3 Pre-Tuned)", "dropdown_cartype"),
            ("EnginePlacementID", "EnginePlacementID", "dropdown_engineplacement"),
            ("MaterialTypeID", "MaterialTypeID", "dropdown_materialtype"),
            ("CurbWeight", "CurbWeight", "num"),
            ("WeightDistribution", "WeightDistribution", "num"),
            ("NumGears", "NumGears", "num"),
            ("TireBrandID", "TireBrandID", "num"),
            ("FrontTireWidthMM", "FrontTireWidthMM", "num"),
            ("FrontTireAspect", "FrontTireAspect", "num"),
            ("FrontWheelDiameterIN", "FrontWheelDiameterIN", "num"),
            ("RearTireWidthMM", "RearTireWidthMM", "num"),
            ("RearTireAspect", "RearTireAspect", "num"),
            ("RearWheelDiameterIN", "RearWheelDiameterIN", "num"),
            ("BaseCost", "BaseCost", "num"),
            ("IsUnicorn", "IsUnicorn (0/1)", "bool"),
        ]

        frm = ttk.Frame(tab)
        frm.pack(fill="both", expand=True, padx=10, pady=10)

        canvas = tk.Canvas(frm, highlightthickness=0)
        canvas.pack(side="left", fill="both", expand=True)
        vs = ttk.Scrollbar(frm, orient="vertical", command=canvas.yview)
        vs.pack(side="right", fill="y")
        canvas.configure(yscrollcommand=vs.set)
        inner = ttk.Frame(canvas)
        canvas.create_window((0, 0), window=inner, anchor="nw")
        inner.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))

        r = 0
        for col, label, kind in fields:
            ttk.Label(inner, text=label).grid(row=r, column=0, sticky="w", pady=4, padx=(0, 10))

            if kind == "bool":
                v = tk.IntVar(value=0)
                self.car_fields[col] = v
                ttk.Checkbutton(inner, variable=v).grid(row=r, column=1, sticky="w")
            elif kind == "dropdown_cartype":
                v = tk.IntVar(value=1)
                self.car_fields[col] = v
                cb = ttk.Combobox(inner, state="readonly",
                                  values=["1 Production", "2 Race", "3 Pre-Tuned"])
                cb.grid(row=r, column=1, sticky="ew")
                cb.bind("<<ComboboxSelected>>", lambda e, var=v, widget=cb: var.set(int(widget.get().split()[0])))
                cb._var_ref = v  # keep
                self._cartype_cb = cb
            elif kind == "dropdown_engineplacement":
                v = tk.IntVar(value=0)
                self.car_fields[col] = v
                cb = ttk.Combobox(inner, state="readonly")
                cb.grid(row=r, column=1, sticky="ew")
                cb._idvar = v
                self._engineplacement_cb = cb
            elif kind == "dropdown_materialtype":
                v = tk.IntVar(value=0)
                self.car_fields[col] = v
                cb = ttk.Combobox(inner, state="readonly")
                cb.grid(row=r, column=1, sticky="ew")
                cb._idvar = v
                self._materialtype_cb = cb

            else:
                v = tk.StringVar(value="")
                self.car_fields[col] = v
                e = ttk.Entry(inner, textvariable=v)
                e.grid(row=r, column=1, sticky="ew")

            r += 1

        inner.columnconfigure(1, weight=1)

        btns = ttk.Frame(tab)
        btns.pack(fill="x", padx=10, pady=(0, 10))
        ttk.Button(btns, text="Load from MAIN", command=self.load_car_fields).pack(side="left")
        ttk.Button(btns, text="Apply to MAIN", command=self.apply_car_fields).pack(side="left", padx=6)

    def _build_tab_body(self):
        tab = ttk.Frame(self.nb)
        self.nb.add(tab, text="Car Body")

        self.body_fields: Dict[str, tk.Variable] = {}
        fields = [
            ("ModelWheelbase", "ModelWheelbase", "num"),
            ("ModelFrontTrackOuter", "ModelFrontTrackOuter", "num"),
            ("ModelRearTrackOuter", "ModelRearTrackOuter", "num"),
            ("ModelFrontStockRideHeight", "ModelFrontStockRideHeight", "num"),
            ("ModelRearStockRideHeight", "ModelRearStockRideHeight", "num"),
        ]

        frm = ttk.Frame(tab)
        frm.pack(fill="both", expand=True, padx=10, pady=10)

        for r, (col, label, kind) in enumerate(fields):
            ttk.Label(frm, text=label).grid(row=r, column=0, sticky="w", pady=4, padx=(0, 10))
            v = tk.StringVar(value="")
            self.body_fields[col] = v
            ttk.Entry(frm, textvariable=v).grid(row=r, column=1, sticky="ew", pady=4)

        frm.columnconfigure(1, weight=1)

        btns = ttk.Frame(tab)
        btns.pack(fill="x", padx=10, pady=(0, 10))
        ttk.Button(btns, text="Load from MAIN", command=self.load_body_fields).pack(side="left")
        ttk.Button(btns, text="Apply to MAIN", command=self.apply_body_fields).pack(side="left", padx=6)

    def _build_tab_engine(self):
        tab = ttk.Frame(self.nb)
        self.nb.add(tab, text="Engine Lab")

        top = ttk.Frame(tab)
        top.pack(fill="x", padx=10, pady=8)

        self.stock_engine_lbl = ttk.Label(top, text="Stock engine: (none loaded)")
        self.stock_engine_lbl.pack(side="left")

        ttk.Button(top, text="Reload stock engine from MAIN", command=self.refresh_stock_engine).pack(side="left", padx=10)

        mid = ttk.PanedWindow(tab, orient="horizontal")
        mid.pack(fill="both", expand=True, padx=10, pady=10)

        left = ttk.Frame(mid)
        mid.add(left, weight=1)

        sr = ttk.Frame(left)
        sr.pack(fill="x", pady=(0, 6))
        ttk.Label(sr, text="Engine search:").pack(side="left")
        self.engine_search_var = tk.StringVar()
        se = ttk.Entry(sr, textvariable=self.engine_search_var)
        se.pack(side="left", fill="x", expand=True, padx=6)
        se.bind("<KeyRelease>", lambda e: self.refresh_engine_list())

        cols = ("EngineID", "EngineName", "MediaName", "Source")
        self.engine_tree = ttk.Treeview(left, columns=cols, show="headings", height=16)
        for c in cols:
            self.engine_tree.heading(c, text=c)
            self.engine_tree.column(c, width=120 if c != "EngineName" else 280, anchor="w")
        self.engine_tree.pack(fill="both", expand=True)

        ys = ttk.Scrollbar(left, orient="vertical", command=self.engine_tree.yview)
        self.engine_tree.configure(yscrollcommand=ys.set)
        ys.place(in_=self.engine_tree, relx=1.0, rely=0, relheight=1.0, anchor="ne")

        self.engine_tree.bind("<<TreeviewSelect>>", self.on_engine_select)

        right = ttk.Frame(mid)
        mid.add(right, weight=2)

        # Assign controls
        assign = ttk.LabelFrame(right, text="Assign / Clone")
        assign.pack(fill="x", pady=(0, 8))

        ttk.Button(
            assign,
            text="Assign selected engine as STOCK (to selected car)",
            command=self.assign_selected_engine_as_stock,
        ).pack(side="left", padx=6, pady=6)

        ttk.Button(
            assign,
            text="Clone selected engine into MAIN",
            command=self.clone_selected_engine_into_main,
        ).pack(side="left", padx=6, pady=6)
        
        ttk.Label(assign, text="New EngineID:").pack(side="left", padx=(20, 6))
        self.new_engine_id_var = tk.IntVar(value=2000)
        ttk.Entry(assign, textvariable=self.new_engine_id_var, width=10).pack(side="left")
        ttk.Button(assign, text="Suggest", command=self._suggest_engine_id).pack(side="left", padx=6)



        # Engine editor fields
        editor = ttk.LabelFrame(right, text="Engine Editor")
        editor.pack(fill="both", expand=True)

        self.engine_fields: Dict[str, tk.Variable] = {}
        engine_cols = [
            ("EngineMass-kg", 'EngineMass-kg', "num"),
            ("MediaName", "MediaName", "dropdown_engine_medianame"),
            ("ConfigID", "ConfigID", "dropdown_engine_config"),
            ("CylinderID", "CylinderID", "dropdown_engine_cylinders"),
            ("Compression", "Compression", "num"),
            ("VariableTimingID", "VariableTimingID", "dropdown_engine_vtiming"),
            ("StockBoost-bar", 'StockBoost-bar', "num"),
            ("MomentInertia", "MomentInertia", "num"),
            ("GasTankSize", "GasTankSize", "num"),
            ("TorqueSteerLeftSpeedScale", "TorqueSteerLeftSpeedScale", "num"),
            ("TorqueSteerRightSpeedScale", "TorqueSteerRightSpeedScale", "num"),
            ("EngineGraphingMaxTorque", "EngineGraphingMaxTorque", "num"),
            ("EngineGraphingMaxPower", "EngineGraphingMaxPower", "num"),
            ("EngineName", "EngineName", "text"),
            ("EngineRotation", "EngineRotation", "num"),
            ("Carbureted", "Carbureted (0/1)", "bool"),
            ("Diesel", "Diesel (0/1)", "bool"),
            ("Rotary", "Rotary (0/1)", "bool"),
        ]

        frm = ttk.Frame(editor)
        frm.pack(fill="both", expand=True, padx=10, pady=10)

        canvas = tk.Canvas(frm, highlightthickness=0)
        canvas.pack(side="left", fill="both", expand=True)
        vs = ttk.Scrollbar(frm, orient="vertical", command=canvas.yview)
        vs.pack(side="right", fill="y")
        canvas.configure(yscrollcommand=vs.set)
        inner = ttk.Frame(canvas)
        canvas.create_window((0, 0), window=inner, anchor="nw")
        inner.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))

        r = 0
        for col, label, kind in engine_cols:
            ttk.Label(inner, text=label).grid(row=r, column=0, sticky="w", pady=4, padx=(0, 10))

            if kind == "bool":
                v = tk.IntVar(value=0)
                self.engine_fields[col] = v
                ttk.Checkbutton(inner, variable=v).grid(row=r, column=1, sticky="w")

            elif kind == "dropdown_engine_medianame":
                v = tk.StringVar(value="")
                self.engine_fields[col] = v
                cb = ttk.Combobox(inner, textvariable=v, state="readonly")
                cb.grid(row=r, column=1, sticky="ew")
                self._engine_medianame_cb = cb

            elif kind == "dropdown_engine_config":
                v = tk.IntVar(value=0)
                self.engine_fields[col] = v
                cb = ttk.Combobox(inner, state="readonly")
                cb.grid(row=r, column=1, sticky="ew")
                cb._idvar = v
                self._engine_config_cb = cb

            elif kind == "dropdown_engine_cylinders":
                v = tk.IntVar(value=0)
                self.engine_fields[col] = v
                cb = ttk.Combobox(inner, state="readonly")
                cb.grid(row=r, column=1, sticky="ew")
                cb._idvar = v
                self._engine_cylinders_cb = cb

            elif kind == "dropdown_engine_vtiming":
                v = tk.IntVar(value=0)
                self.engine_fields[col] = v
                cb = ttk.Combobox(inner, state="readonly")
                cb.grid(row=r, column=1, sticky="ew")
                cb._idvar = v
                self._engine_vtiming_cb = cb

            else:
                v = tk.StringVar(value="")
                self.engine_fields[col] = v
                ttk.Entry(inner, textvariable=v).grid(row=r, column=1, sticky="ew")

            r += 1

        inner.columnconfigure(1, weight=1)



        btns = ttk.Frame(editor)
        btns.pack(fill="x", padx=10, pady=(0, 10))
        ttk.Button(btns, text="Load engine fields from MAIN", command=self.load_engine_fields).pack(side="left")
        ttk.Button(btns, text="Apply engine edits to MAIN", command=self.apply_engine_fields).pack(side="left", padx=6)
        
 


    def _build_tab_upgrades(self):
        tab = ttk.Frame(self.nb)
        self.nb.add(tab, text="Upgrade and Misc Editor")

        top = ttk.Frame(tab)
        top.pack(fill="x", padx=10, pady=8)

        ttk.Label(top, text="Table:").pack(side="left")
        self.table_var = tk.StringVar(value="")
        self.table_cb = ttk.Combobox(top, textvariable=self.table_var, state="readonly", values=[])
        self.table_cb.pack(side="left", padx=6)
        self._table_display_to_real = {}
        self._table_real_to_display = {}

        ttk.Button(top, text="Refresh table list", command=self.refresh_table_list).pack(side="left", padx=6)

        ttk.Button(top, text="Load rows (Ordinal=car)", command=self.load_table_rows).pack(side="left", padx=6)

        mid = ttk.PanedWindow(tab, orient="horizontal")
        mid.pack(fill="both", expand=True, padx=10, pady=10)

        left = ttk.Frame(mid)
        mid.add(left, weight=1)

        self.rows_tree = ttk.Treeview(left, columns=("__rowid__", "Level", "IsStock"), show="headings", height=14)
        self.rows_tree.heading("__rowid__", text="rowid")
        self.rows_tree.heading("Level", text="Upgrade Level")
        self.rows_tree.heading("IsStock", text="Stock? (1=yes,0=upgrade)")
        self.rows_tree.column("__rowid__", width=80, anchor="w")
        self.rows_tree.column("Level", width=80, anchor="w")
        self.rows_tree.column("IsStock", width=80, anchor="w")
        self.rows_tree.pack(fill="both", expand=True)

        ys = ttk.Scrollbar(left, orient="vertical", command=self.rows_tree.yview)
        self.rows_tree.configure(yscrollcommand=ys.set)
        ys.place(in_=self.rows_tree, relx=1.0, rely=0, relheight=1.0, anchor="ne")

        self.rows_tree.bind("<<TreeviewSelect>>", self.on_row_select)

        row_btns = ttk.Frame(left)
        row_btns.pack(fill="x", pady=(8, 0))
        ttk.Button(row_btns, text="Add row (copy selected)", command=self.add_row_copy).pack(side="left")
        ttk.Button(row_btns, text="Delete selected row", command=self.delete_selected_row).pack(side="left", padx=6)

        right = ttk.Frame(mid)
        mid.add(right, weight=2)

        self.field_frame = ttk.LabelFrame(right, text="Row fields (edit any column)")
        self.field_frame.pack(fill="both", expand=True)

        self.field_canvas = tk.Canvas(self.field_frame, highlightthickness=0)
        self.field_canvas.pack(side="left", fill="both", expand=True)
        self.field_scroll = ttk.Scrollbar(self.field_frame, orient="vertical", command=self.field_canvas.yview)
        self.field_scroll.pack(side="right", fill="y")
        self.field_canvas.configure(yscrollcommand=self.field_scroll.set)

        self.fields_inner = ttk.Frame(self.field_canvas)
        self.field_canvas.create_window((0, 0), window=self.fields_inner, anchor="nw")
        self.fields_inner.bind("<Configure>", lambda e: self.field_canvas.configure(scrollregion=self.field_canvas.bbox("all")))

        self.current_row_fields: Dict[str, tk.StringVar] = {}
        self.current_row_rowid: Optional[int] = None
        self.current_row_full: Dict[str, Any] = {}

        bottom = ttk.Frame(tab)
        bottom.pack(fill="x", padx=10, pady=(0, 10))
        ttk.Button(bottom, text="Apply row edits (MAIN)", command=self.apply_row_edits).pack(side="left")

    def _build_tab_constructor(self):
        tab = ttk.Frame(self.nb)
        self.nb.add(tab, text="Constructor (Donor subsystems)")

        wrap = ttk.Frame(tab)
        wrap.pack(fill="both", expand=True, padx=10, pady=10)

        info = ttk.Label(
            wrap,
            text="This tab applies subsystem upgrade definitions from a donor car (MAIN or DLC) to the selected target car (MAIN only).\n"
                 "It reuses your existing constructor_engine subsystem copier logic."
        )
        info.pack(anchor="w", pady=(0, 10))

        line = ttk.Frame(wrap)
        line.pack(fill="x", pady=6)

        ttk.Label(line, text="Donor car ID:").pack(side="left")
        self.donor_id_var = tk.StringVar(value="")
        ttk.Entry(line, textvariable=self.donor_id_var, width=10).pack(side="left", padx=6)

        ttk.Label(line, text="Donor source:").pack(side="left")
        self.donor_source_var = tk.StringVar(value="")
        self.donor_source_cb = ttk.Combobox(line, textvariable=self.donor_source_var, state="readonly", values=[])
        self.donor_source_cb.pack(side="left", padx=6, fill="x", expand=True)

        ttk.Label(line, text="Subsystem:").pack(side="left")
        self.subsystem_var = tk.StringVar(value="Engine")
        self.subsystem_cb = ttk.Combobox(line, textvariable=self.subsystem_var, state="readonly", values=[
            "Engine", "SpringDamper", "Transmission", "Differential", "Brakes", "Tires", "Aero"
        ])
        self.subsystem_cb.pack(side="left", padx=6)

        ttk.Label(line, text="Level:").pack(side="left")
        self.level_var = tk.StringVar(value="0")
        ttk.Entry(line, textvariable=self.level_var, width=6).pack(side="left", padx=6)

        ttk.Button(wrap, text="Apply donor subsystem to selected car (writes MAIN)",
                   command=self.apply_subsystem).pack(anchor="w", pady=6)

        self.constructor_log = tk.Text(wrap, height=18, wrap="word")
        self.constructor_log.pack(fill="both", expand=True, pady=10)

    # ----------------------------
    # Source selection
    # ----------------------------
    
    def pick_main(self):
        fp = filedialog.askopenfilename(title="Select MAIN SLT", filetypes=[("SLT/SQLite", "*.slt *.db *.sqlite"), ("All files", "*.*")])
        if not fp:
            return
        self.main_db = Path(fp)
        self.main_lbl.configure(text=f"MAIN: {self.main_db.name}")
        self.reload_sources()

    def pick_dlc(self):
        d = filedialog.askdirectory(title="Select DLC folder containing SLTs")
        if not d:
            return
        self.dlc_folder = Path(d)
        self.dlc_lbl.configure(text=f"DLC: {self.dlc_folder.name}")
        self.reload_sources()

    def reload_sources(self):
        if not self.main_db:
            return
        self.sources = ce.build_source_list(self.main_db, self.dlc_folder)
        self._log(f"Loaded sources: {len(self.sources)}\n")
        self.refresh_car_list()
        self.refresh_engine_list()
        self.refresh_table_list()
        self._refresh_donor_sources()
        # auto-build lookups so dropdowns aren't empty
        self.lookup_cache = ce.build_lookup_cache(self.main_db, self.sources)
        self._refresh_dropdowns()


    def rebuild_cache(self):
        if not self.main_db:
            messagebox.showwarning("Missing MAIN", "Select MAIN SLT first.")
            return
        self.lookup_cache = ce.build_lookup_cache(self.main_db, self.sources)
        self._log("Lookup cache rebuilt.\n")
        self._refresh_dropdowns()

    def _refresh_dropdowns(self):
        # EnginePlacement (id = EnginePlacement, name = DisplayName)
        ep = self.lookup_cache.get("List_EnginePlacement", {})
        if hasattr(self, "_engineplacement_cb"):
            vals = [f"{k} - {v}" for k, v in sorted(ep.items())]
            self._engineplacement_cb.configure(values=vals)
            self._engineplacement_cb.bind(
                "<<ComboboxSelected>>",
                lambda e, cb=self._engineplacement_cb: cb._idvar.set(int(cb.get().split(" - ")[0]))
            )

        # MaterialType (id = MaterialTypeID, name = Material)
        mt = self.lookup_cache.get("List_MaterialType", {})
        if hasattr(self, "_materialtype_cb"):
            vals = [f"{k} - {v}" for k, v in sorted(mt.items())]
            self._materialtype_cb.configure(values=vals)
            self._materialtype_cb.bind(
                "<<ComboboxSelected>>",
                lambda e, cb=self._materialtype_cb: cb._idvar.set(int(cb.get().split(" - ")[0]))
            )
            
        # Engine Config dropdown: List_EngineConfig (EngineConfig -> DisplayName)
        ec = self.lookup_cache.get("List_EngineConfig", {})
        if hasattr(self, "_engine_config_cb"):
            vals = [f"{k} - {v}" for k, v in sorted(ec.items())]
            self._engine_config_cb.configure(values=vals)
            self._engine_config_cb.bind(
                "<<ComboboxSelected>>",
                lambda e, cb=self._engine_config_cb: cb._idvar.set(int(cb.get().split(" - ")[0]))
            )

        # Cylinders: List_Cylinders or List_Cylinder (CylinderID -> Number)
        cyl = self.lookup_cache.get("List_Cylinders", {}) or self.lookup_cache.get("List_Cylinder", {})
        if hasattr(self, "_engine_cylinders_cb"):
            vals = [f"{k} - {v}" for k, v in sorted(cyl.items())]
            self._engine_cylinders_cb.configure(values=vals)
            self._engine_cylinders_cb.bind(
                "<<ComboboxSelected>>",
                lambda e, cb=self._engine_cylinders_cb: cb._idvar.set(int(cb.get().split(" - ")[0]))
            )

        # Variable timing: List_VariableTiming (VariableTimingID -> VariableTimingType)
        vt = self.lookup_cache.get("List_VariableTiming", {})
        if hasattr(self, "_engine_vtiming_cb"):
            vals = [f"{k} - {v}" for k, v in sorted(vt.items())]
            self._engine_vtiming_cb.configure(values=vals)
            self._engine_vtiming_cb.bind(
                "<<ComboboxSelected>>",
                lambda e, cb=self._engine_vtiming_cb: cb._idvar.set(int(cb.get().split(" - ")[0]))
            )

        # Engine MediaName dropdown: unique MediaName values from all sources' Data_Engine
        if hasattr(self, "_engine_medianame_cb"):
            names = ce.list_distinct_engine_medianames(self.sources)
            self._engine_medianame_cb.configure(values=names)



        # CarType
        if hasattr(self, "_cartype_cb"):
            # set by load_car_fields
            pass

    # ----------------------------
    # Cars list
    # ----------------------------
    def refresh_car_list(self):
        for i in self.car_tree.get_children():
            self.car_tree.delete(i)

        if not self.sources:
            return

        q = (self.car_search_var.get() or "").strip().lower()
        only_clones = bool(self.only_clones_var.get())
        sort_by = self.car_sort_var.get() or "CarID"

        cars = ce.list_cars_all_sources(self.sources)

        # filter
        out = []
        for c in cars:
            if q and q not in (c["MediaName"] or "").lower() and q not in str(c["CarID"]).lower():
                continue
            if only_clones:
                y = c.get("Year")
                if y == 6969:
                    pass
                else:
                    if c["CarID"] < 2000:
                        continue
            out.append(c)

        # sort
        if sort_by == "MediaName":
            out.sort(key=lambda x: (x.get("MediaName") or "", x["CarID"]))
        elif sort_by == "Year":
            out.sort(key=lambda x: (x.get("Year") or 0, x["CarID"]))
        elif sort_by == "Source":
            out.sort(key=lambda x: (x.get("Source") or "", x["CarID"]))
        else:
            out.sort(key=lambda x: x["CarID"])

        for c in out[:5000]:
            self.car_tree.insert("", "end", values=(c["CarID"], c.get("MediaName", ""), c.get("Year", ""), Path(c["Source"]).name))

        self._log(f"Cars listed: {len(out)} (showing up to 5000)\n")

    def on_car_select(self, _evt=None):
        sel = self.car_tree.selection()
        if not sel:
            return
        vals = self.car_tree.item(sel[0], "values")
        car_id = int(vals[0])
        source_name = vals[3]
        # resolve source path
        src = None
        for p in self.sources:
            if p.name == source_name:
                src = p
                break
        self.selected_car_id = car_id
        self.selected_car_source = src
        self._log(f"Selected car: {car_id} ({vals[1]}) from {source_name}\n")

        # Auto-load car/body from MAIN only if exists there
        self.load_car_fields()
        self.load_body_fields()
        self.refresh_stock_engine()
        self.refresh_table_list()

    # ----------------------------
    # Engines list
    # ----------------------------
    def refresh_engine_list(self):
        for i in self.engine_tree.get_children():
            self.engine_tree.delete(i)
        if not self.sources:
            return

        q = (self.engine_search_var.get() or "").strip().lower()
        engines = ce.list_engines_all_sources(self.sources)

        out = []
        for e in engines:
            if q and q not in (e.get("EngineName") or "").lower() and q not in (e.get("MediaName") or "").lower() and q not in str(e["EngineID"]).lower():
                continue
            out.append(e)

        out.sort(key=lambda x: (x["EngineID"], x.get("Source") or ""))

        for e in out[:5000]:
            self.engine_tree.insert("", "end", values=(e["EngineID"], e.get("EngineName", ""), e.get("MediaName", ""), Path(e["Source"]).name))

    def on_engine_select(self, event=None):
        sel = self.engine_tree.selection()
        if not sel:
            return
        vals = self.engine_tree.item(sel[0], "values")

        try:
            self.selected_engine_id = int(vals[0])
        except Exception:
            self.selected_engine_id = None

        # vals[3] is Source (might be only filename)
        src = ""
        try:
            src = str(vals[3])
        except Exception:
            src = ""

        self.selected_engine_source = self._resolve_source_path(src)



    # ----------------------------
    # Data_Car load/apply
    # ----------------------------
    def load_car_fields(self):
        if not self.main_db or self.selected_car_id is None:
            return

        row = ce.get_data_car(self.main_db, self.selected_car_id)
        if not row:
            self._log("Car not found in MAIN (write-only). Clone it first if needed.\n")
            return

        for k, var in self.car_fields.items():
            if k not in row:
                continue
            if isinstance(var, tk.IntVar):
                try:
                    var.set(int(row[k] if row[k] is not None else 0))
                except Exception:
                    var.set(0)
            else:
                var.set("" if row[k] is None else str(row[k]))

        # Update CarType combobox text
        if hasattr(self, "_cartype_cb"):
            v = self.car_fields.get("CarTypeID")
            if isinstance(v, tk.IntVar):
                cur = v.get()
                mapping = {1: "1 Production", 2: "2 Race", 3: "3 Pre-Tuned"}
                self._cartype_cb.set(mapping.get(cur, f"{cur}"))

        # Update dropdowns display (EnginePlacement, MaterialType)
        self._set_dropdown_display("_engineplacement_cb", "EnginePlacementID", "List_EnginePlacement")
        self._set_dropdown_display("_materialtype_cb", "MaterialTypeID", "List_MaterialType")

    def _set_dropdown_display(self, cb_attr: str, field: str, table: str):
        cb = getattr(self, cb_attr, None)
        if not cb:
            return
        v = self.car_fields.get(field)
        if not isinstance(v, tk.IntVar):
            return
        id_val = v.get()
        disp = self.lookup_cache.get(table, {}).get(id_val)
        if disp is None:
            return
        cb.set(f"{id_val} - {disp}")
        
    def _set_engine_dropdown_display(self, cb_attr: str, field: str, table: str):
        cb = getattr(self, cb_attr, None)
        if not cb:
            return
        v = self.engine_fields.get(field)
        if not isinstance(v, tk.IntVar):
            return
        id_val = v.get()
        disp = self.lookup_cache.get(table, {}).get(id_val)
        if disp is None:
            return
        cb.set(f"{id_val} - {disp}")


    def apply_car_fields(self):
        if not self.main_db or self.selected_car_id is None:
            messagebox.showwarning("Missing selection", "Select a car that exists in MAIN.")
            return
        updates: Dict[str, Any] = {}
        for k, var in self.car_fields.items():
            if isinstance(var, tk.IntVar):
                updates[k] = int(var.get())
            else:
                s = (var.get() or "").strip()
                if s == "":
                    continue
                # Keep numeric as numeric where possible
                try:
                    if "." in s:
                        updates[k] = float(s)
                    else:
                        updates[k] = int(s)
                except Exception:
                    updates[k] = s

        try:
            ce.backup_db(self.main_db)
            ce.update_data_car(self.main_db, self.selected_car_id, updates)
        except Exception as e:
            messagebox.showerror("Apply failed", str(e))
            return
        self._log("Data_Car updated in MAIN.\n")

    # ----------------------------
    # Data_CarBody load/apply
    # ----------------------------
    def load_body_fields(self):
        if not self.main_db or self.selected_car_id is None:
            return
        row = ce.get_data_carbody_for_car(self.main_db, self.selected_car_id)
        if not row:
            self._log("CarBody not found in MAIN for this car.\n")
            return
        self._carbody_id = row.get("Id")
        for k, var in self.body_fields.items():
            if k not in row:
                continue
            var.set("" if row[k] is None else str(row[k]))

    def apply_body_fields(self):
        if not self.main_db or self.selected_car_id is None:
            messagebox.showwarning("Missing selection", "Select a car that exists in MAIN.")
            return
        body = ce.get_data_carbody_for_car(self.main_db, self.selected_car_id)
        if not body:
            messagebox.showwarning("Missing body", "No Data_CarBody row found for this car in MAIN.")
            return
        carbody_id = int(body["Id"])
        updates: Dict[str, Any] = {}
        for k, var in self.body_fields.items():
            s = (var.get() or "").strip()
            if s == "":
                continue
            try:
                updates[k] = float(s) if "." in s else int(s)
            except Exception:
                updates[k] = s

        try:
            ce.backup_db(self.main_db)
            ce.update_data_carbody(self.main_db, carbody_id, updates)
        except Exception as e:
            messagebox.showerror("Apply failed", str(e))
            return
        self._log("Data_CarBody updated in MAIN.\n")

    # ----------------------------
    # Stock engine (List_UpgradeEngine)
    # ----------------------------
    def refresh_stock_engine(self):
        if not self.main_db or self.selected_car_id is None:
            return
        eng = ce.get_stock_engine_for_car(self.main_db, self.selected_car_id)
        if not eng:
            self.stock_engine_lbl.configure(text="Stock engine: (not found in MAIN)")
            return
        eid = eng.get("EngineID")
        ename = ce.resolve_engine_name(self.sources, int(eid)) if eid is not None else ""
        self.stock_engine_lbl.configure(text=f"Stock engine: {eid}  {ename}")

    def assign_selected_engine_as_stock(self):
        if not self.main_db or self.selected_car_id is None:
            messagebox.showwarning("Missing selection", "Select a target car (in MAIN) first.")
            return
        if self.selected_engine_id is None:
            messagebox.showwarning("Missing engine", "Select an engine first.")
            return

        # Only assign engines that exist in MAIN
        if not ce.engine_exists_in_main(self.main_db, self.selected_engine_id):
            messagebox.showwarning(
                "Engine not in MAIN",
                "Selected engine does not exist in MAIN. Use 'Clone engine into MAIN + Assign' first."
            )
            return

        try:
            ce.backup_db(self.main_db)
            ce.set_stock_engine_for_car(self.main_db, self.selected_car_id, self.selected_engine_id)
        except Exception as e:
            messagebox.showerror("Assign failed", str(e))
            return
        self._log(f"Assigned EngineID {self.selected_engine_id} as stock for CarID {self.selected_car_id}\n")
        self.refresh_stock_engine()

    def clone_engine_then_assign(self):
        if not self.main_db or self.selected_car_id is None:
            messagebox.showwarning("Missing selection", "Select a target car (in MAIN) first.")
            return
        if self.selected_engine_id is None or self.selected_engine_source is None:
            messagebox.showwarning("Missing engine", "Select an engine first.")
            return

        if ce.engine_exists_in_main(self.main_db, self.selected_engine_id):
            # Already in main, just assign
            self.assign_selected_engine_as_stock()
            return

        if clone_engine_to_main is None or suggest_next_engine_id is None:
            messagebox.showwarning(
                "Cloner engine not available",
                "Could not import clone_engine_to_main/suggest_next_engine_id from cloner_engine.py.\n"
                "Place constructor_app.py in the same folder as cloner_engine.py."
            )
            return

            # Clone engine into MAIN using safe EngineID >= 2000
            try:
                if not self.selected_engine_source:
                    raise ValueError("Engine source path is missing (select an engine first).")

                # Resolve source path robustly (handles cases where Source is just a filename)
                src = Path(str(self.selected_engine_source))
                if not src.exists():
                    # match by basename against loaded sources
                    match = next((p for p in self.sources if p.name.lower() == src.name.lower()), None)
                    if not match:
                        raise ValueError(f"Could not resolve engine source file: {self.selected_engine_source}")
                    src = match

                # Backup MAIN first
                ce.backup_db(self.main_db)

                # Suggest next EngineID considering MAIN + DLC sources
                new_eid = suggest_next_engine_id(
                    main_db=self.main_db,
                    aux_sources=self.sources,   # pass the list of Paths
                    min_id=2000,
                )

                # Clone engine (and let engine cloner also use all sources if it supports it)
                clone_engine_to_main(
                    source_db=src,
                    main_db=self.main_db,
                    source_engine_id=int(self.selected_engine_id),
                    new_engine_id=int(new_eid),
                    all_source_paths=self.sources,  # if your clone_engine_to_main supports it
                )

            except Exception as e:
                messagebox.showerror("Clone engine failed", str(e))
                return


        # Assign new engine
        try:
            ce.set_stock_engine_for_car(self.main_db, self.selected_car_id, new_eid)
        except Exception as e:
            messagebox.showerror("Assign failed", str(e))
            return

        self._log(f"Cloned Engine {self.selected_engine_id} -> {new_eid} into MAIN and assigned to CarID {self.selected_car_id}\n")
        self.refresh_stock_engine()

    # ----------------------------
    # Engine editor (Data_Engine)
    # ----------------------------
    def load_engine_fields(self):
        if not self.main_db:
            return

        # Prefer selected engine, else use stock engine
        eid = self.selected_engine_id
        if eid is None and self.selected_car_id is not None:
            eng = ce.get_stock_engine_for_car(self.main_db, self.selected_car_id)
            if eng and eng.get("EngineID") is not None:
                eid = int(eng["EngineID"])

        if eid is None:
            messagebox.showwarning("No engine", "Select an engine or load a car with a stock engine.")
            return

        if not ce.engine_exists_in_main(self.main_db, eid):
            messagebox.showwarning("Not in MAIN", "This engine does not exist in MAIN. Clone it first.")
            return

        row = ce.get_data_engine(self.main_db, eid)
        if not row:
            messagebox.showwarning("Not found", "Engine row not found in MAIN.")
            return

        for k, var in self.engine_fields.items():
            if k not in row:
                continue
            if isinstance(var, tk.IntVar):
                try:
                    var.set(int(row[k] if row[k] is not None else 0))
                except Exception:
                    var.set(0)
            else:
                var.set("" if row[k] is None else str(row[k]))
                
        # set dropdown visible labels (id - name) for engine dropdowns
        self._set_engine_dropdown_display("_engine_config_cb", "ConfigID", "List_EngineConfig")
        if hasattr(self, "_engine_cylinders_cb"):
            table = "List_Cylinders" if "List_Cylinders" in self.lookup_cache else "List_Cylinder"
            self._set_engine_dropdown_display("_engine_cylinders_cb", "CylinderID", table)
        self._set_engine_dropdown_display("_engine_vtiming_cb", "VariableTimingID", "List_VariableTiming")

    def apply_engine_fields(self):
        if not self.main_db:
            return

        eid = self.selected_engine_id
        if eid is None and self.selected_car_id is not None:
            eng = ce.get_stock_engine_for_car(self.main_db, self.selected_car_id)
            if eng and eng.get("EngineID") is not None:
                eid = int(eng["EngineID"])
        if eid is None:
            messagebox.showwarning("No engine", "Select an engine or load a car with a stock engine.")
            return

        if not ce.engine_exists_in_main(self.main_db, eid):
            messagebox.showwarning("Not in MAIN", "This engine does not exist in MAIN. Clone it first.")
            return

        updates: Dict[str, Any] = {}
        for k, var in self.engine_fields.items():
            if isinstance(var, tk.IntVar):
                updates[k] = int(var.get())
            else:
                s = (var.get() or "").strip()
                if s == "":
                    continue
                try:
                    updates[k] = float(s) if "." in s else int(s)
                except Exception:
                    updates[k] = s

        try:
            ce.backup_db(self.main_db)
            ce.update_data_engine(self.main_db, eid, updates)
        except Exception as e:
            messagebox.showerror("Apply failed", str(e))
            return
        self._log("Data_Engine updated in MAIN.\n")

    # ----------------------------
    # List_* / upgrade table editor
    # ----------------------------
    def refresh_table_list(self):
        if not self.main_db:
            self.table_cb.configure(values=[])
            return

        real_tables = ce.list_car_related_tables(self.main_db)

        self._table_display_to_real = {}
        self._table_real_to_display = {}

        display = []
        for t in real_tables:
            tl = t
            if t.startswith("List_Upgrade"):
                tl = t.replace("List_Upgrade", "", 1)
            elif t.startswith("List_"):
                tl = t.replace("List_", "", 1)
            self._table_display_to_real[tl] = t
            self._table_real_to_display[t] = tl
            display.append(tl)

        display.sort(key=lambda x: (0 if x.lower().startswith("engine") or x.lower().startswith("drivetrain") else 1, x.lower()))

        self.table_cb.configure(values=display)
        if display and not self.table_var.get():
            self.table_var.set(display[0])


    def load_table_rows(self):
        if not self.main_db or self.selected_car_id is None:
            messagebox.showwarning("Missing selection", "Select a target car first.")
            return

        table = self._table_display_to_real.get(self.table_var.get(), self.table_var.get())
        if not table:
            return

        # Resolve scope values from MAIN (write-only target)
        engine_id = None
        carbody_id = None
        drivetrain_id = None
        
        try:
            drivetrain_id = ce.get_stock_drivetrain_id_for_car(self.main_db, self.selected_car_id)
        except Exception:
            drivetrain_id = None

        try:
            stock = ce.get_stock_engine_for_car(self.main_db, self.selected_car_id)
            if stock:
                eid = stock.get("EngineID") or stock.get("EngineId") or stock.get("Engine")
                if eid is not None:
                    engine_id = int(eid)
        except Exception:
            engine_id = None

        try:
            body = ce.get_data_carbody_for_car(self.main_db, self.selected_car_id)
            if body and body.get("Id") is not None:
                carbody_id = int(body["Id"])
        except Exception:
            carbody_id = None

        rows, scope_kind, scope_col, scope_val = ce.list_rows_scoped(
            self.main_db,
            table,
            self.selected_car_id,
            engine_id=engine_id,
            carbody_id=carbody_id,
            drivetrain_id=drivetrain_id,
        )

        for i in self.rows_tree.get_children():
            self.rows_tree.delete(i)

        for r in rows:
            lvl = r.get("Level", "")
            st = r.get("IsStock", "")
            self.rows_tree.insert("", "end", values=(r["__rowid__"], lvl, st))

        self.current_row_fields.clear()
        self.current_row_rowid = None
        self.current_row_full = {}
        self._clear_fields_panel()

        # Log what scope was used (super helpful for debugging “why no rows”)
        if scope_kind is None:
            self._log(f"No supported scope column found for {table}. (Not Ordinal/CarID/EngineID/CarBodyID)\n")
        elif scope_val is None:
            self._log(f"{table} is {scope_kind}-scoped ({scope_col}), but scope value is missing for this car.\n")
        else:
            self._log(f"Loaded {len(rows)} rows from {table} using {scope_kind} scope: {scope_col}={scope_val}\n")



    def on_row_select(self, _evt=None):
        sel = self.rows_tree.selection()
        if not sel:
            return
        rowid = int(self.rows_tree.item(sel[0], "values")[0])
        table = self._table_display_to_real.get(self.table_var.get(), self.table_var.get())

        if not self.main_db or not table:
            return
        if self.selected_car_id is None:
            return
        row = ce.get_row_by_rowid(self.main_db, table, rowid)
        if not row:
            return
        self.current_row_rowid = rowid
        self.current_row_full = row
        self._build_fields_panel(row)

    def _build_fields_panel(self, row: Dict[str, Any]):
        self._clear_fields_panel()
        self.current_row_fields = {}

        # IMPORTANT: use real table name, not display label
        table = self._table_display_to_real.get(self.table_var.get(), self.table_var.get())

        r = 0
        for k, v in row.items():
            if k == "__rowid__":
                continue

            ttk.Label(self.fields_inner, text=k).grid(row=r, column=0, sticky="w", pady=2, padx=(0, 10))

            widget = None

            # Wheel diameters clamp 13..24
            if "wheeldiameter" in k.lower():
                sv = tk.StringVar(value="" if v is None else str(v))
                self.current_row_fields[k] = sv
                sp = tk.Spinbox(self.fields_inner, from_=13, to=24, textvariable=sv, width=10)
                sp.grid(row=r, column=1, sticky="w", pady=2)
                widget = sp

            # TireCompound dropdown
            elif table.lower() == "list_upgradetirecompound" and k == "TireCompoundID":
                opts = self.lookup_cache.get("List_TireCompound", {})
                sv = tk.StringVar(value="" if v is None else str(v))
                self.current_row_fields[k] = sv
                cb = ttk.Combobox(self.fields_inner, state="readonly",
                                values=[f"{i} - {n}" for i, n in sorted(opts.items())])
                cb.grid(row=r, column=1, sticky="ew", pady=2)
                if v is not None:
                    try:
                        iv = int(v)
                        if iv in opts:
                            cb.set(f"{iv} - {opts[iv]}")
                    except Exception:
                        pass
                cb.bind("<<ComboboxSelected>>", lambda e, cb=cb, sv=sv: sv.set(cb.get().split(" - ")[0]))
                widget = cb

            # Drivetrain EngineID dropdown (all engines across sources)
            elif table.lower() == "list_upgradedrivetrain" and k == "EngineID":
                engines = ce.list_engines_all_sources(self.sources)
                vals = [f'{e["EngineID"]} - {e.get("EngineName","")} ({Path(e["Source"]).name})' for e in engines]
                sv = tk.StringVar(value="" if v is None else str(v))
                self.current_row_fields[k] = sv
                cb = ttk.Combobox(self.fields_inner, state="readonly", values=vals)
                cb.grid(row=r, column=1, sticky="ew", pady=2)
                if v is not None:
                    try:
                        iv = int(v)
                        for s in vals:
                            if s.startswith(f"{iv} -"):
                                cb.set(s)
                                break
                    except Exception:
                        pass
                cb.bind("<<ComboboxSelected>>", lambda e, cb=cb, sv=sv: sv.set(cb.get().split(" - ")[0]))
                widget = cb

            # Drivetrain PowertrainID dropdown (Data_Drivetrain resolver)
            elif table.lower() == "list_upgradedrivetrain" and k == "PowertrainID":
                opts = ce.build_powertrain_options(self.sources, self.lookup_cache)
                sv = tk.StringVar(value="" if v is None else str(v))
                self.current_row_fields[k] = sv
                cb = ttk.Combobox(self.fields_inner, state="readonly",
                                values=[f"{pid} - {label}" for pid, label in opts])
                cb.grid(row=r, column=1, sticky="ew", pady=2)
                if v is not None:
                    try:
                        iv = int(v)
                        for pid, label in opts:
                            if pid == iv:
                                cb.set(f"{pid} - {label}")
                                break
                    except Exception:
                        pass
                cb.bind("<<ComboboxSelected>>", lambda e, cb=cb, sv=sv: sv.set(cb.get().split(" - ")[0]))
                widget = cb

            if widget is None:
                sv = tk.StringVar(value="" if v is None else str(v))
                self.current_row_fields[k] = sv
                e = ttk.Entry(self.fields_inner, textvariable=sv)
                e.grid(row=r, column=1, sticky="ew", pady=2)

            r += 1

        self.fields_inner.columnconfigure(1, weight=1)


    def _clear_fields_panel(self):
        for w in self.fields_inner.winfo_children():
            w.destroy()

    def apply_row_edits(self):
        if not self.main_db or self.selected_car_id is None:
            return
        table = self._table_display_to_real.get(self.table_var.get(), self.table_var.get())
        if not table or self.current_row_rowid is None:
            return
        updates: Dict[str, Any] = {}
        for k, var in self.current_row_fields.items():
            s = (var.get() or "").strip()
            # allow blank -> NULL
            if s == "":
                updates[k] = None
                continue
            try:
                updates[k] = float(s) if "." in s else int(s)
            except Exception:
                updates[k] = s

        try:
            ce.backup_db(self.main_db)
            ce.update_row_by_rowid(self.main_db, table, self.current_row_rowid, updates)
        except Exception as e:
            messagebox.showerror("Apply failed", str(e))
            return

        self._log(f"Updated {table} rowid={self.current_row_rowid}\n")
        self.load_table_rows()

    def add_row_copy(self):
        if not self.main_db or self.selected_car_id is None:
            return
        table = self._table_display_to_real.get(self.table_var.get(), self.table_var.get())
        if not table:
            return
        if self.current_row_rowid is None:
            messagebox.showwarning("No row", "Select a row to copy first.")
            return
        row = ce.get_row_by_rowid(self.main_db, table, self.current_row_rowid)
        if not row:
            return

        # Copy, but remove rowid, and enforce Ordinal = selected car (if column exists)
        vals = dict(row)
        vals.pop("__rowid__", None)
        if "Ordinal" in vals:
            vals["Ordinal"] = self.selected_car_id

        # If it has Level, ask for new level number (simple prompt)
        if "Level" in vals:
            new_level = tk.simpledialog.askinteger("New level", "Enter Level value for the new row:", initialvalue=int(vals.get("Level") or 0))
            if new_level is None:
                return
            vals["Level"] = int(new_level)

        try:
            ce.backup_db(self.main_db)
            ce.insert_row(self.main_db, table, vals)
        except Exception as e:
            messagebox.showerror("Insert failed", str(e))
            return

        self._log(f"Inserted new row into {table}\n")
        self.load_table_rows()

    def delete_selected_row(self):
        if not self.main_db:
            return
        table = self._table_display_to_real.get(self.table_var.get(), self.table_var.get())
        if not table or self.current_row_rowid is None:
            return
        if not messagebox.askyesno("Delete row", f"Delete rowid={self.current_row_rowid} from {table}?"):
            return
        try:
            ce.backup_db(self.main_db)
            ce.delete_row_by_rowid(self.main_db, table, self.current_row_rowid)
        except Exception as e:
            messagebox.showerror("Delete failed", str(e))
            return
        self._log(f"Deleted row from {table}\n")
        self.load_table_rows()

    # ----------------------------
    # Constructor donor apply
    # ----------------------------
    def _refresh_donor_sources(self):
        if not self.sources:
            self.donor_source_cb.configure(values=[])
            return
        vals = [p.name for p in self.sources]
        self.donor_source_cb.configure(values=vals)
        if vals and not self.donor_source_var.get():
            self.donor_source_var.set(vals[0])

    def apply_subsystem(self):
        if not self.main_db or self.selected_car_id is None:
            messagebox.showwarning("Missing target", "Select a target car first (must exist in MAIN).")
            return

        donor_id_s = (self.donor_id_var.get() or "").strip()
        if not donor_id_s.isdigit():
            messagebox.showwarning("Missing donor", "Enter a donor car ID.")
            return

        donor_car_id = int(donor_id_s)
        donor_src_name = self.donor_source_var.get()
        donor_src = None
        for p in self.sources:
            if p.name == donor_src_name:
                donor_src = p
                break
        if donor_src is None:
            messagebox.showwarning("Missing donor source", "Select a donor source SLT.")
            return

        subsystem = self.subsystem_var.get()
        try:
            level = int((self.level_var.get() or "0").strip())
        except Exception:
            level = 0

        try:
            rep = ce.apply_subsystem_from_donor(
                main_db=self.main_db,
                donor_db=donor_src,
                target_car_id=self.selected_car_id,
                donor_car_id=donor_car_id,
                subsystem=subsystem,
                level=level,
            )
        except Exception as e:
            messagebox.showerror("Apply failed", str(e))
            return

        self.constructor_log.insert("end", f"{rep}\n")
        self.constructor_log.see("end")

    # ----------------------------
    # Backup / validate
    # ----------------------------
    def backup_main(self):
        if not self.main_db:
            return
        try:
            b = ce.backup_db(self.main_db)
        except Exception as e:
            messagebox.showerror("Backup failed", str(e))
            return
        self._log(f"Backup created: {b}\n")

    def validate_selected_car(self):
        if not self.main_db or self.selected_car_id is None:
            return
        try:
            issues = ce.basic_validate_car(self.main_db, self.selected_car_id)
        except Exception as e:
            messagebox.showerror("Validate failed", str(e))
            return
        if not issues:
            messagebox.showinfo("Validation", "No basic issues found.")
        else:
            messagebox.showwarning("Validation issues", "\n".join(issues))

    # ----------------------------
    # Logging
    # ----------------------------
    def _log(self, s: str):
        self.log.insert("end", s)
        self.log.see("end")


if __name__ == "__main__":
    ConstructorApp().mainloop()
