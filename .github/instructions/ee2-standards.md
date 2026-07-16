---
description: NCO HPC Environment Equivalence (EE2) Standards
applyTo: "**/*"
---

# NOAA NWS Office of Modeling and Development - EE2 Standards

## 1. Core Principles & Error Handling
* **Strict EE2 Compliance:** All code must strictly conform to the NCO HPC Environment Equivalence (EE2) Implementation Standards.
* **Reference Documentation:** For authoritative guidance, see the [NWS HPC Standards documentation](https://nws-hpc-standards.readthedocs.io/en/stable/).
* **Descriptive Error Messages:** Fatal errors must print a descriptive message beginning strictly with `FATAL ERROR:`. Warnings or non-fatal messages must begin with `WARNING:`.
* **Zero False Errors:** Eliminate false errors in output logs (e.g., suppressing standard "No such file or directory" or syntax errors). Output must be clean.
* **Appropriate Failure Modes:** Executables must not terminate abnormally (e.g., segfaults) for discoverable errors. Validate the existence of required input or restart data *before* running executables.
* **Backup & Opportunity Data:**
  * If primary data is missing but backup data is used, the script must log a `WARNING:` and gracefully proceed.
  * *Data of Opportunity:* Code must not fail when non-24/7 supported data sources are missing.

## 2. Directory Structure & Execution Rules
* **Operational Paths Only:** Code must NEVER use hard-coded absolute paths. Rely entirely on standard environment variables set by the module system (`$DATA`, `$COMROOT`, `$EXECmodel`, `$USHmodel`, etc.).
* **Output Destinations:** Output must only be written to `$DATA`, `com`, or `nwges`.
  * **DEPRECATED PATHS:** Never write to `pcom` (use the `wmo` sub-directory under `com`) or `com-nawips` (use the `gempak` sub-directory under `com`).
* **No Background Processing:** Never put processes in the background using `&`. The workload manager (PBS Pro/Slurm) loses control of backgrounded processes.
* **No External Symlinks:** Do not generate code that creates symbolic links pointing outside of the application directory or package. Use variables defined in the version file/ecf script instead.
* **Restarts:** Any job running longer than 15 minutes MUST have restart/checkpoint capability built-in to pick up exactly where it left off.

## 3. Operational Scripting Architecture (The "J-Job" Structure)
Code must adhere strictly to the NCO multi-tiered scripting architecture. Variables must be exported down the chain, never passed as command-line arguments.
* **Job Cards (ecFlow scripts):** Submits the job to the scheduler (PBS/Slurm). Loads required modules and sets root environment variables (`$envir`, `$OPSROOT`). Calls the J-Job.
* **J-Jobs (`jobs/` directory):**
  * **Naming:** Must follow the convention `JAAAAA` (Capital `J` followed by all caps, no file extension).
  * **Purpose:** Sets up location variables (`$DATA`, `$COMIN`, `$COMOUT`, `$EXECmodel`, etc.) and temporal variables (`$PDY`, `$cycle`), initializes the working directory, and calls the ex-script.
  * **Strict Rule:** Variables established in the J-Job must *never* be altered by downstream scripts. Keep the J-Job clutter-free (do not define unused variables).
* **Ex-Scripts (`scripts/` directory):** The primary execution layer for a specific task. Called by the J-Job.
* **Ush-Scripts (`ush/` directory):** Utility scripts called by ex-scripts to handle repeated specific tasks.

## 4. GRIB Utilities & File Naming (Appendix E)
* **GRIB Utility Access:** NCO supports manipulating GRIB data exclusively via the `grib_util` and `wgrib2` modules. These must be loaded in the job cards. Do not use hardcoded paths to GRIB utilities.
* **GRIB Inventories:** ASCII inventory files (output of `wgrib`/`wgrib2`) MUST end with the extension `.grib2.idx` (e.g., `hrrr.t10z.wrfnatf01.grib2.idx`). Other binary index files must end with `.bin.idx`.
* **Strict NCEP File Naming:**
  * Use periods (`.`) to separate categories and underscores (`_`) to separate words.
  * Use a `p` for decimals in grid resolutions (e.g., `0.25` becomes `0p25`). Use a leading `0` for resolutions < 1.
  * Include an `f` in front of the forecast hour and pad with zeros (e.g., `f01`, `f006`).
  * If output is before the cycle time, substitute `tm` for `f`.
  * Multiple pieces of `var_info` should be separated by periods (e.g., `gefs.t06z.avg.pres_a.0p50.f006.grib2`).

## 5. Language & Compilation Specifics
* **Makefiles:** Shared packages must be backward compatible. All Makefiles MUST include exactly these four targets: `all`, `debug`, `install`, and `clean`.
* **Interpreted Scripts (Bash/Python):**
  * Rely on the `prod_util` module to add utilities to the user's `$PATH`.
  * Never alter the `$PATH` by appending `.` or `$USHmodel`.
  * For Python, suppress `FutureWarnings` to prevent operational log clutter. Never rely on the system Python; always use the module system.
* **Docblocks:** All source files and scripts must include a standard NCO DOCBLOCK header (Program Name, Author, Abstract, History Log, Usage, Input/Output files).
