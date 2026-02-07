import sys
import time
import signal
import logging
import subprocess
import os
import threading
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%H:%M:%S',
    stream=sys.stdout
)
logger = logging.getLogger("Orchestrator")

IGNORED_PHRASES = [
    "NativeCodeLoader",
    "incubator modules",
    "SparkUI",
    "log4j",
    "Utils: Your hostname",
    "Utils: Set SPARK_LOCAL_IP",
    "NumExpr defaulting",
    "Unable to load native-hadoop",
    "Setting default log level",
    "To adjust logging level",
    "loading settings",
    "resolving dependencies",
    "retrieving ::",
    "modules in use:",
    "artifacts copied",
    "postgresql",
    "checker-qual"
]

class LogFilter(threading.Thread):
    def __init__(self, process):
        super().__init__()
        self.process = process
        self.daemon = True

    def run(self):
        for line in iter(self.process.stderr.readline, b''):
            line_str = line.decode('utf-8', errors='replace')
            if not any(phrase in line_str for phrase in IGNORED_PHRASES):
                sys.stderr.write(line_str)
                sys.stderr.flush()

class ServiceOrchestrator:
    def __init__(self):
        self.project_root = Path(__file__).resolve().parent.parent
        self.processes = []
        
        self.batch_sequence = [
            "src/pipelines/silver/slv_btch_dims_load.py",
            "src/pipelines/gold/gld_btch_dims_load.py"
        ]
        
        self.streaming_services = [
            "src/generator/gen_telemetry.py",
            "src/pipelines/gold/gld_live_fleet.py",
            "src/pipelines/gold/gld_live_orders.py",
            "src/pipelines/gold/gld_speed_layer_fact_perf.py"
        ]
        self._running = True

    def _run_process_filtered(self, command, env, cwd, wait=False):
        try:
            proc = subprocess.Popen(
                command,
                cwd=cwd,
                env=env,
                stderr=subprocess.PIPE,
                stdout=subprocess.DEVNULL
            )

            log_thread = LogFilter(proc)
            log_thread.start()

            if wait:
                proc.wait()
                if proc.returncode != 0:
                    raise subprocess.CalledProcessError(proc.returncode, command)
                return None
            else:
                return proc

        except Exception as e:
            logger.error(f"Failed to execute {command}: {e}")
            if wait:
                raise e
            return None

    def run_batch_sequence(self):
        logger.info(">>> STEP 1: Starting Batch Initialization Sequence (Silver -> Gold)")
        env = os.environ.copy()
        python_exec = sys.executable

        for script in self.batch_sequence:
            full_path = self.project_root / script
            
            if not full_path.exists():
                logger.error(f"CRITICAL: Batch script missing: {script}")
                sys.exit(1)

            logger.info(f"--- Executing Batch: {script} ---")
            try:
                self._run_process_filtered(
                    [python_exec, str(full_path)],
                    cwd=self.project_root,
                    env=env,
                    wait=True
                )
                logger.info(f"--- Finished Batch: {script} (Success) ---")
            except subprocess.CalledProcessError as e:
                logger.error(f"Batch execution failed for {script} (Exit Code: {e.returncode})")
                logger.error("Aborting sequence. Fix batch errors before running streaming.")
                sys.exit(1)

    def start_streaming_services(self):
        logger.info(">>> STEP 2: Starting Real-Time Speed Layer Services")
        env = os.environ.copy()
        python_exec = sys.executable

        for script_path in self.streaming_services:
            full_path = self.project_root / script_path
            
            if not full_path.exists():
                logger.error(f"Script missing: {script_path}")
                continue

            proc = self._run_process_filtered(
                [python_exec, str(full_path)],
                cwd=self.project_root,
                env=env,
                wait=False
            )
            
            if proc:
                self.processes.append((proc, script_path))
                logger.info(f"Started Service: {script_path} [PID: {proc.pid}]")
                time.sleep(2) 

    def run(self):
        self.run_batch_sequence()
        self.start_streaming_services()
        self.monitor()

    def monitor(self):
        logger.info(">>> System Running. Press Ctrl+C to stop.")
        try:
            while self._running and self.processes:
                time.sleep(1)
                for proc, name in self.processes[:]:
                    ret_code = proc.poll()
                    if ret_code is not None:
                        logger.warning(f"Process terminated unexpectedly: {name} (Exit Code: {ret_code})")
                        self.processes.remove((proc, name))
                
                if not self.processes:
                    logger.info("No active services remaining.")
                    self._running = False
                    
        except KeyboardInterrupt:
            self.shutdown()

    def shutdown(self, signum=None, frame=None):
        if not self._running: 
            return
            
        self._running = False
        print() 
        logger.info("Graceful shutdown initiated...")

        for proc, name in self.processes:
            if proc.poll() is None:
                logger.info(f"Stopping {name}...")
                proc.terminate()
                try:
                    proc.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    logger.warning(f"Force killing {name}")
                    proc.kill()

        logger.info("System halted.")
        sys.exit(0)

if __name__ == "__main__":
    orchestrator = ServiceOrchestrator()
    
    signal.signal(signal.SIGINT, orchestrator.shutdown)
    signal.signal(signal.SIGTERM, orchestrator.shutdown)

    orchestrator.run()