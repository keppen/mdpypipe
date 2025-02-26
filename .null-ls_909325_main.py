import logging
import time
from pathlib import Path
from typing import Any, Callable, Dict, List

import context
import logger
import pipeline as pip
from interfaces import NextStep, PipeStepInterface
from shell_commands import CheckProgerss, RunMD, RunSLURM
from topology import (
    PrepareMDP,
    ReadBox,
    ReadPositions,
    ReadTopology,
    WriteParameters,
    WritePositions,
)


def init_file_check_routine(
    context: context.MDContext, next_step: NextStep
) -> None: ...


def context_setup_routine(context: context.MDContext, next_step: NextStep) -> None:
    log1 = logging.getLogger(__name__)
    log1.info("### STARTING CONTEXT SETUP ROUTINE ###")

    # READ ROUTINE

    context.move_files()

    jobs: List[PipeStepInterface] = []

    file = context.GEOMETRY_POSITIONS_FILE
    job1 = ReadPositions(file)
    jobs.append(job1)

    file = context.GEOMETRY_BOX_FILE
    job2 = ReadBox(file)
    jobs.append(job2)

    for topology in context.TOP_CONFIG:
        file = topology["file"]
        ff = topology["ff"]
        name = topology["name"]
        times = topology["number"]

        job = ReadTopology(name=name, file=file, ff=ff, times=times)
        jobs.append(job)

    # WRITE ROUTINE #
    basename = context.TITLE_BASENAME
    software = context.TITLE_SOFTWARE

    job3 = WriteParameters(basename, software)
    job4 = WritePositions(basename, software)
    jobs.extend([job3, job4])

    # RUNNING PIPELINE #
    pipe: pip.Pipeline = pip.Pipeline(*jobs)
    pipe(context)

    next_step(context)


def run_routine(context: context.MDContext, next_step: NextStep) -> None:
    print("### STARTING RUN ROUTINE ###")

    runMD_configs: List[Dict[str, Any]] = context.RUNMD_CONFIG
    jobs: List[Callable] = []

    if len(runMD_configs) == 1:
        single_config = runMD_configs[0]
        if context.TITLE_SOFTWARE == "gromacs":
            prepare_mdp = PrepareMDP(single_config["file"])
            jobs.append(prepare_mdp)

        for i in range(single_config["nruns"]):
            single_config["number"] = i
            run = RunMD(**single_config)
            run.gen_command()
            jobs.append(run)

    else:
        for single_config in runMD_configs:
            if context.TITLE_SOFTWARE == "gromacs":
                prepare_mdp = PrepareMDP(single_config["file"])
                jobs.append(prepare_mdp)

            run = RunMD(**single_config)
            run.gen_command()
            jobs.append(run)

    slurm_config = context.SLURM_CONFIG
    job9 = RunSLURM(**slurm_config)
    job9.gen_command()

    # RUNNING PIPELINE #
    pipe: pip.Pipeline = pip.Pipeline(*jobs, job9)
    pipe(context)

    next_step(context)


def rerun_routine(context: context.MDContext, next_step: NextStep) -> None:
    print("### STARTING RERUN ROUTINE ###")

    runs = context.find_unfinished()
    print(runs[columns])

    data_dir = context.PATHS_DATA_DIR
    software = context.TITLE_SOFTWARE

    pipe_jobs: List[Callable] = []
    for index, run in runs.iterrows():
        sim_name = run.loc["SIMULATION NAME"]
        positions_file = data_dir / run.loc["POSITIONS FILE"]
        topology_file = data_dir / run.loc["TOPOLOGY FILE"]
        config_file = data_dir / run.loc["CONFIG FILE"]
        if software == "gromacs":
            prepare_mdp = PrepareMDP(config_file)
            pipe_jobs.append(prepare_mdp)
        number, sim_type = sim_name.split("-")

        config = {
            "sim_type": sim_type,
            "software": software,
            "number": int(number),
            "file": config_file,
            "topology_file": topology_file,
            "positions_file": positions_file,
        }

        run = RunMD(**config)
        run.gen_command()
        pipe_jobs.append(run)

    slurm_config = context.SLURM_CONFIG
    job9 = RunSLURM(**slurm_config)
    job9.gen_command()

    # RUNNING PIPELINE #
    pipe: pip.Pipeline = pip.Pipeline(*pipe_jobs, job9)
    pipe(context)

    next_step(context)


def check_runs_routine(context: context.MDContext, next_step: NextStep) -> None:
    def filter_logs(file_name: Path) -> bool:
        ext = file_name.suffix
        if ext in [".log", ".mdout"]:
            return True
        return False

    print("### STARTING CHECK RUNS ROUTINE ###")

    log_list = filter(filter_logs, list(context.PATHS_DATA_DIR.iterdir()))

    pipe_jobs = []
    for log_file in list(log_list):
        log_file = context.PATHS_DATA_DIR / log_file
        print(log_file)
        pipe_jobs.append(CheckProgerss(log_file))

    pipe: pip.Pipeline = pip.Pipeline(*pipe_jobs)
    pipe(context)

    print(context.DATABASE.database[columns])

    next_step(context)


def watch_queue_routine(context: context.MDContext, next_step: NextStep) -> None:
    if not hasattr(context, "PID"):
        context.pid = context.DATABASE.find_entries(
            **{"PROJECT NAME": context.TITLE_PROJECT_NAME}
        )["PID"].max()

    print("WATCH QUEUE ROUTINE")

    while True:
        process = context.SSH_CONNECTION.run_remotely(
            f"/usr/sbin/squeue --jobs={context.pid}"
        )

        pipe: pip.Pipeline = pip.Pipeline(
            download_logs,
            check_runs_routine,
            download_finished,
        )
        pipe(context)

        if process.returncode != 0:
            break

        current_time_seconds = time.time()

        new_time_seconds = current_time_seconds + 15 * 60
        new_time_struct = time.localtime(new_time_seconds)
        new_time_str = time.strftime("%Y-%m-%d %H:%M:%S", new_time_struct)

        print("Next check: ", new_time_str)
        time.sleep(900)

    next_step(context)


def remote_run_routine(context: context.MDContext, next_step: NextStep) -> None:
    print("### STARTING REMOTE RUN PROCEDURE ###")

    context.SSH_CONNECTION.run_remotely(f"mkdir -p {context.PATHS_REMOTE_DIR}")

    context.SSH_CONNECTION.send_files(
        f"{context.PATHS_DATA_DIR}/*",
        f"{context.PATHS_REMOTE_ADRESS}:{context.PATHS_REMOTE_DIR}",
    )

    process = context.SSH_CONNECTION.run_remotely(
        f"sbatch {context.PATHS_REMOTE_DIR}/md.slurm"
    )

    context.change_pid(int(process.stdout.split()[3]))

    context.DATABASE.save()

    next_step(context)


def download_logs(context: context.MDContext, next_step: NextStep) -> None:
    context.SSH_CONNECTION.send_files(
        f"{context.PATHS_REMOTE_ADRESS}:{context.PATHS_REMOTE_DIR}/" + "*.{log,mdout}",
        f"{context.PATHS_DATA_DIR}/",
    )
    next_step(context)


def download_finished(context: context.MDContext, next_step: NextStep) -> None:
    runs = context.DATABASE.find_entries(**{"PROJECT NAME": context.TITLE_PROJECT_NAME})
    sim_names: List[str] = []

    for _, run in runs.iterrows():
        if run["STAGE"] == "Finished":
            sim_names.append(run["SIMULATION NAME"])

    files = "{%s}.*" % (",".join(sim_names))

    context.SSH_CONNECTION.send_files(
        f"{context.PATHS_REMOTE_ADRESS}:{context.PATHS_REMOTE_DIR}/" + files,
        f"{context.PATHS_DATA_DIR}/",
    )

    context.SSH_CONNECTION.run_remotely(f"rm {context.PATHS_REMOTE_DIR}/{files}")

    next_step(context)


if __name__ == "__main__":
    # logger()
    import os
    import re

    root = "/home/keppen/MD/parameters/implicit_config"
    for config in os.listdir(root):
        if not re.search(r"amber\.config$", config):
            continue

        print(config)

        context_config = Path(f"{root}/{config}")

        test_context = context.MDContext.from_config(context_config)

        test_context.remove_file("md.run")

        columns = [
            "PROJECT NAME",
            "SIMULATION NAME",
            "TOPOLOGY FILE",
            "POSITIONS FILE",
            "CONFIG FILE",
            "STAGE",
            "PID",
        ]
        print(test_context.DATABASE.database[columns])

        pipe: pip.Pipeline = pip.Pipeline(
            context_setup_routine,
            run_routine,
            # remote_run_routine,
            # check_runs_routine,
            # watch_queue_routine,
        )
        pipe(test_context)

        test_context.DATABASE.save()
        print(test_context.DATABASE.database[columns])
