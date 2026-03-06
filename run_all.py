"""
ULK Final Project — run_all.py
===============================
Runs all project parts using  python run_all.py
Does NOT require spark-submit or SPARK_HOME to be set.
Uses PySpark installed via pip (pyspark package).

Requirements:
    pip install pyspark==3.5.1 faker pandas pymongo matplotlib seaborn plotly

Usage:
    python run_all.py            # runs all parts in order
    python run_all.py --part 2a  # runs one specific part
"""

import subprocess, sys, os, argparse, time

# ── Detect python executable ──────────────────────────────────────────────────
def find_python():
    for cmd in ["py", "python3", "python"]:
        try:
            out = subprocess.check_output([cmd, "--version"],
                                          stderr=subprocess.STDOUT).decode()
            if "Python 3" in out:
                return cmd
        except Exception:
            pass
    return None

PYTHON = find_python()
if not PYTHON:
    print("\n  ERROR: Python 3 not found.")
    print("  Download from: https://www.python.org/downloads/")
    print("  IMPORTANT: Tick 'Add Python to PATH' during install!\n")
    sys.exit(1)

# ── Patch all Spark scripts to use local mode via pip pyspark ─────────────────
PYSPARK_LOCAL_CONFIG = """
import os
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
"""

# ── Runner ────────────────────────────────────────────────────────────────────
def run(label: str, script: str, extra_env: dict = None):
    if not os.path.exists(script):
        print(f"\n  SKIP  {label} — {script} not found")
        return True

    env = os.environ.copy()
    env["PYSPARK_PYTHON"]        = sys.executable
    env["PYSPARK_DRIVER_PYTHON"] = sys.executable
    # Tell PySpark to use local mode
    env["SPARK_LOCAL_IP"]    = "127.0.0.1"
    env["SPARK_LOCAL_DIRS"]  = "C:\\spark_tmp"
    env["TMPDIR"]            = "C:\\spark_tmp"
    if extra_env:
        env.update(extra_env)

    print(f"\n{'='*60}")
    print(f"  Running: {label}")
    print(f"{'='*60}")
    t0 = time.time()

    result = subprocess.run([PYTHON, script], env=env)

    elapsed = time.time() - t0
    if result.returncode == 0:
        print(f"\n  OK  {label} completed in {elapsed:.1f}s")
        return True
    else:
        print(f"\n  ERROR  {label} failed (exit code {result.returncode})")
        return False


def check_packages():
    """Install missing pip packages automatically."""
    required = ["pyspark", "faker", "pandas", "pymongo",
                 "matplotlib", "seaborn", "plotly"]
    missing  = []
    for pkg in required:
        try:
            __import__(pkg)
        except ImportError:
            missing.append(pkg)

    if missing:
        print(f"\n  Installing missing packages: {', '.join(missing)}")
        subprocess.check_call(
            [PYTHON, "-m", "pip", "install"] + missing + ["--quiet"]
        )
        print("  OK  Packages installed")
    else:
        print("  OK  All required packages already installed")


def check_project_files():
    required = ["dataset_generator.py", "part2a_data_cleaning.py",
                 "part2b_batch_analytics.py", "part2c_stream_simulation.py",
                 "part3_analytics_integration.py", "part4_visualizations.py"]
    missing = [f for f in required if not os.path.exists(f)]
    if missing:
        print("\n  ERROR: Missing project files:")
        for f in missing:
            print(f"    - {f}")
        print("\n  Make sure all scripts are in the same folder as run_all.py")
        sys.exit(1)
    print("  OK  All project scripts found")


# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="ULK Project Runner")
    parser.add_argument("--part", choices=["gen","2a","2b","2c","3","4","all"],
                        default="all", help="Which part to run")
    args = parser.parse_args()

    print("=" * 60)
    print("  ULK E-Commerce Analytics — Project Runner")
    print(f"  Python: {PYTHON}  ({sys.executable})")
    print("=" * 60)

    print("\n[Pre-flight checks]")
    check_packages()
    check_project_files()

    parts = {
        "gen": ("Generate Dataset",         "dataset_generator.py"),
        "2a":  ("Part 2a: Data Cleaning",   "part2a_data_cleaning.py"),
        "2b":  ("Part 2b: Batch Analytics", "part2b_batch_analytics.py"),
        "2c":  ("Part 2c: Stream Sim",      "part2c_stream_simulation.py"),
        "3":   ("Part 3: Integration",      "part3_analytics_integration.py"),
        "4":   ("Part 4: Visualizations",   "part4_visualizations.py"),
    }
    order = ["gen", "2a", "2b", "2c", "3", "4"]

    if args.part != "all":
        order = [args.part]

    # Skip dataset generation if JSON files already exist
    if "gen" in order and os.path.exists("users.json"):
        print("\n  SKIP  Dataset already exists (delete users.json to regenerate)")
        order = [p for p in order if p != "gen"]

    failed = []
    for key in order:
        label, script = parts[key]
        ok = run(label, script)
        if not ok:
            failed.append(label)
            print(f"\n  Stopping — fix the error above then re-run:")
            print(f"    python run_all.py --part {key}")
            break

    print("\n" + "=" * 60)
    if not failed:
        print("  ALL PARTS COMPLETED SUCCESSFULLY!")
        print("\n  Output folders:")
        for folder in ["cleaned", "analytics", "streaming", "integration", "charts"]:
            if os.path.isdir(folder):
                print(f"    {folder}/")
    else:
        print(f"  FAILED: {', '.join(failed)}")
    print("=" * 60)


if __name__ == "__main__":
    main()