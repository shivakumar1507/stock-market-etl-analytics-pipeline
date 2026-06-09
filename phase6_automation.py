# ============================================================
# Stock Market ETL Pipeline — Phase 6: Automation
# Project : Automated Financial Data Pipeline
# Author  : Shiva Kumar Devatha
# Tools   : Python (schedule, subprocess, json)
# Purpose : Schedules full pipeline to run daily at 06:00 AM
#           Phase 1 (Extract) -> Phase 2 (Transform) -> Phase 3 (Load)
# ============================================================
# SETUP — run once in terminal before this script:
#   pip install schedule
# ============================================================

import os
import sys
import json
import logging
import schedule
import time
import subprocess
from datetime import datetime

# ============================================================
# 1. CONFIGURATION
# ============================================================

BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
LOG_DIR    = os.path.join(BASE_DIR, "logs")
STATUS_FILE = os.path.join(BASE_DIR, "pipeline_status.json")

# Schedule time — 06:00 AM daily
SCHEDULE_TIME = "06:00"

# Pipeline phases — order matters, runs sequentially
PIPELINE_PHASES = [
    ("Phase 1 — Extractor",   "phase1_extractor.py"),
    ("Phase 2 — Transformer", "phase2_transformer.py"),
    ("Phase 3 — Loader",      "phase3_loader.py"),
]

# ============================================================
# 2. SETUP — FOLDERS & LOGGING
# ============================================================

os.makedirs(LOG_DIR, exist_ok=True)

log_filename = os.path.join(
    LOG_DIR,
    f"automation_{datetime.today().strftime('%Y%m%d_%H%M%S')}.log"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)s]  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# ============================================================
# 3. STATUS FILE WRITER
# ============================================================

def write_run_status(status, duration_seconds, failed_phase=None, notes=""):
    """
    Write last pipeline run result to pipeline_status.json.
    This file acts as a quick health check — readable without
    opening logs. Also visible in GitHub repo as proof of run.
    """
    payload = {
        "last_run"      : datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "status"        : status,
        "duration_secs" : round(duration_seconds, 2),
        "failed_phase"  : failed_phase if failed_phase else "None",
        "notes"         : notes,
        "next_run"      : f"Tomorrow at {SCHEDULE_TIME}",
        "schedule"      : f"Daily at {SCHEDULE_TIME}"
    }

    with open(STATUS_FILE, "w") as f:
        json.dump(payload, f, indent=4)

    logger.info(f"Pipeline status written -> {STATUS_FILE}")


# ============================================================
# 4. PHASE RUNNER
# ============================================================

def run_phase(phase_name, script_filename):
    """
    Run a single pipeline phase as a subprocess.
    Returns True on success, False on failure.

    Using subprocess.run so each phase runs in its own
    Python process — exactly how production schedulers work.
    """
    script_path = os.path.join(BASE_DIR, script_filename)

    # Verify script exists before attempting to run
    if not os.path.exists(script_path):
        logger.error(f"{phase_name} — Script not found: {script_path}")
        return False

    logger.info(f"Starting {phase_name} ...")
    phase_start = datetime.now()

    result = subprocess.run(
        [sys.executable, script_path],
        capture_output = True,
        text           = True
    )

    duration = (datetime.now() - phase_start).total_seconds()

    if result.returncode == 0:
        logger.info(
            f"{phase_name} — SUCCESS "
            f"({duration:.1f}s)"
        )
        return True
    else:
        logger.error(
            f"{phase_name} — FAILED "
            f"({duration:.1f}s)"
        )
        # Log the actual error output from the failed phase
        if result.stderr:
            logger.error(f"Error details:\n{result.stderr.strip()}")
        if result.stdout:
            logger.error(f"Last output:\n{result.stdout.strip()[-500:]}")
        return False


# ============================================================
# 5. MASTER PIPELINE RUNNER
# ============================================================

def run_pipeline():
    """
    Master orchestrator — runs all phases in sequence.

    Design decisions:
    - Fail-fast: pipeline halts immediately if any phase fails
    - Full logging: every phase result logged with duration
    - Status file: written after every run (success or failure)
    - Audit trail: Phase 3 already writes to etl_audit_log table
    """
    pipeline_start = datetime.now()
    run_timestamp  = pipeline_start.strftime("%Y-%m-%d %H:%M:%S")

    logger.info("=" * 60)
    logger.info("AUTOMATED PIPELINE RUN STARTED")
    logger.info("=" * 60)
    logger.info(f"Run timestamp : {run_timestamp}")
    logger.info(f"Phases        : {len(PIPELINE_PHASES)}")
    logger.info(f"Schedule      : Daily at {SCHEDULE_TIME}")
    logger.info("=" * 60)

    failed_phase = None

    # ── Run each phase in order ───────────────────────────────
    for phase_name, script_filename in PIPELINE_PHASES:
        success = run_phase(phase_name, script_filename)

        if not success:
            # Fail-fast — don't run subsequent phases
            failed_phase = phase_name
            logger.error(f"Pipeline halted at: {phase_name}")
            logger.error("Fix the error above and re-run phase6_automation.py")
            break

    # ── Calculate total duration ──────────────────────────────
    total_duration = (datetime.now() - pipeline_start).total_seconds()
    pipeline_success = failed_phase is None

    # ── Final summary ─────────────────────────────────────────
    logger.info("")
    logger.info("=" * 60)
    logger.info("PIPELINE RUN SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Status        : {'SUCCESS' if pipeline_success else 'FAILED'}")
    logger.info(f"Total duration: {total_duration:.1f} seconds")
    logger.info(f"Failed phase  : {failed_phase if failed_phase else 'None'}")
    logger.info(f"Log file      : {log_filename}")
    logger.info("=" * 60)

    # ── Write status file ─────────────────────────────────────
    write_run_status(
        status         = "SUCCESS" if pipeline_success else "FAILED",
        duration_seconds = total_duration,
        failed_phase   = failed_phase,
        notes          = (
            "All phases completed successfully."
            if pipeline_success
            else f"Pipeline halted at {failed_phase}. Check logs."
        )
    )

    if pipeline_success:
        logger.info("Pipeline complete. Database and dashboard data are up to date.")
    else:
        logger.error("Pipeline failed. Database NOT updated for this run.")

    return pipeline_success


# ============================================================
# 6. SCHEDULER SETUP
# ============================================================

def start_scheduler():
    """
    Start the daily scheduler.
    Runs the pipeline once immediately on start,
    then schedules it for every day at SCHEDULE_TIME.
    """
    logger.info("=" * 60)
    logger.info("STOCK MARKET ETL PIPELINE — PHASE 6: AUTOMATION")
    logger.info("=" * 60)
    logger.info(f"Scheduler     : Daily at {SCHEDULE_TIME}")
    logger.info(f"Base dir      : {BASE_DIR}")
    logger.info(f"Status file   : {STATUS_FILE}")
    logger.info(f"Log file      : {log_filename}")
    logger.info("=" * 60)

    # ── Run immediately on start (for testing + first load) ───
    logger.info("Running pipeline immediately for initial verification ...")
    logger.info("")
    run_pipeline()

    # ── Schedule daily run ────────────────────────────────────
    schedule.every().day.at(SCHEDULE_TIME).do(run_pipeline)

    logger.info("")
    logger.info(f"Scheduler active. Next run: today/tomorrow at {SCHEDULE_TIME}")
    logger.info("Keep this script running to maintain the schedule.")
    logger.info("To stop: press Ctrl+C")
    logger.info("")

    # ── Keep alive loop ───────────────────────────────────────
    # Checks every 60 seconds if a scheduled job is due
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)

    except KeyboardInterrupt:
        logger.info("")
        logger.info("Scheduler stopped manually (Ctrl+C).")
        logger.info("Pipeline will not run automatically until restarted.")


# ============================================================
# 7. UTILITY — CHECK LAST RUN STATUS
# ============================================================

def check_status():
    """
    Print the last pipeline run status from pipeline_status.json.
    Useful for quickly checking if the pipeline ran successfully.
    """
    if not os.path.exists(STATUS_FILE):
        print("[INFO] No pipeline run recorded yet. Run phase6_automation.py first.")
        return

    with open(STATUS_FILE, "r") as f:
        status = json.load(f)

    print("\n── LAST PIPELINE RUN STATUS ──")
    for key, value in status.items():
        print(f"  {key:<20} : {value}")
    print("")


# ============================================================
# 8. RUN
# ============================================================

if __name__ == "__main__":

    # Support two modes:
    # python phase6_automation.py          -> starts scheduler
    # python phase6_automation.py status   -> shows last run status

    if len(sys.argv) > 1 and sys.argv[1] == "status":
        check_status()
    else:
        start_scheduler()