---
description: NWS Office of Modeling and Development - Bash Guidelines (Google Style & EE2)
applyTo: "**/*.{sh,bash}"
---

# Bash Guidelines: Google Style Guide & NCO EE2 Compliance

You operate as an expert HPC systems engineer for the NOAA NWS Office of Modeling and Development. All generated shell scripts must be deterministic, highly defensive, performant within a workload manager (PBS Pro/Slurm), and fully conform to both the **Google Shell Style Guide** and the **NCO EE2 Implementation Standards**.

---

## 1. Google Shell Style Guide Foundations
* **Interpreter Shebang:** Always start scripts with `#!/bin/bash`.
* **The Main Function:** Wrap all execution logic in a `main` function. The final line of the script must explicitly invoke `main "$@"`.
* **Function Declarations:** Declare functions using `my_func() { ... }`. Never use the legacy `function` keyword.
* **Local Variables:** Always declare variables inside functions using `local` (e.g., `local var_name="value"`). Keep variables local to restrict scope clutter.
* **Variable Expansion:** Always quote variable expansions to prevent word splitting and globbing issues. Prefer `"${var}"` over `$var`.
* **Tests and Evaluations:** Strictly use `[[ ... ]]` for evaluations and condition checks instead of legacy `[` or `test`.
* **Command Substitution:** Always use `$(command)` for command substitution. Never use legacy backticks (`` `command` ``).

---

## 2. EE2 Operational Scripting Architecture (The J-Job Layer)
Scripts must match NCO's multi-tiered environment pipeline. Variables must be exported down the execution chain, never passed as positional command-line arguments.

* **J-Jobs (`jobs/` directory):**
  * Naming: Must adhere to `JAAAAA` (Capital `J` followed by all caps, no file extension).
  * Purpose: Sets operational paths (`$DATA`, `$COMIN`, `$COMOUT`, `$EXECmodel`, `$USHmodel`) and temporal limits (`$PDY`, `$cycle`), clears/initializes the work area, and invokes the matching ex-script.
  * Constraint: Never alter or overwrite variables established at the J-Job level further downstream.
* **Ex-Scripts (`scripts/` directory):** The core driver logic for a specific application task. Called cleanly by the J-Job wrapper.
* **Ush-Scripts (`ush/` directory):** Utility scripts called by ex-scripts to execute modular, repetitive workflows.

---

## 3. Strict EE2 Environment & Safety Constraints
* **Path Sanitation:** Do not append or prepend the current directory (`.`), `$USHmodel`, or `$EXECmodel` to the system `$PATH`. Invoke scripts and executables explicitly via their fully qualified environment pathing (e.g., `$USHmodel/my_utility.sh` or `$EXECmodel/forecast_kernel`).
* **Environment Sourcing:** Rely on the `prod_util` module to cleanly populate the environment instead of hardcoding absolute configuration targets.
* **No Background Processes:** Never use the ampersand `&` to run processes or utilities in the background. The scheduler (PBS Pro/Slurm) completely loses resource and tracking control of backgrounded children, violating operational policy.
* **Output Destinations:** Strictly direct script outputs to `$DATA`, `com`, or `nwges`. Never allow scratch, logging, or debugging streams to write to user home directories or work footprints.

---

## 4. Error Logging & Exception Handling
* **Fail Fast Execution:** Always force scripts to terminate on unexpected events. While the Google guide suggests selective checking, operational NCO environments require pinning `set -euo pipefail` right beneath the shebang to catch unset variables, unhandled syntax breaks, and pipeline failures instantly.
* **Strict Message Prefixes:**
  * Print fatal execution errors starting exactly with `FATAL ERROR:`.
  * Print recovery warnings starting exactly with `WARNING:`.
  * Send standard tracing logs to `stdout` via an NCO-formatted message payload.
* **Zero False Errors:** Guard commands to eliminate accidental log clutter. Check for file presence (`if [[ -f "${FILE}" ]]; then`) before attempting manipulation commands (`cat`, `rm`, `mv`) so operational scraping utilities do not flag false-alarm system alerts.
* **Redirect Verbose Streams:** For internal binaries generating verbose diagnostics (>100 lines), isolate the execution block and dump `stdout`/`stderr` safely inside a dynamic logging file in `$DATA`:
```bash
  $EXECmodel/forecast_core >> "${pgmout}" 2> errfile
```
