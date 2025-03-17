import logging
from os import wait
import time
from pathlib import Path
from typing import Any, Callable, Hashable

import numpy
from pandas import Series

from src.context.context import MDContext
from src.interfaces.datatypes import RunConfig
from src.interfaces.pipeline import NextStep, PipeStep, Pipeline
from src.routines.context import (
    FindRunConfig,
    FindTopolConfig,
    MergeTopologies,
    SetCurrentRun,
    SetCurrentTopology,
)
from src.routines.files import CheckProgress, RunMD, RunSLURM, PrepareMDOptions
from src.routines.structure import (
    ReadCoordinates,
    ReadTopology,
    WriteParameters,
    WriteCoordinates,
)


def topology_setup_routine(context: MDContext, next_step: NextStep) -> None:
    log = logging.getLogger(topology_setup_routine.__name__)
    log.info("Starting topology setup routine.")

    jobs: list[PipeStep[MDContext]] = []

    for topology in context.simulation_menager.topol_configs:
        set_job = SetCurrentTopology(topology)
        read_job = ReadTopology()
        jobs.extend([set_job, read_job])

    job0: PipeStep[MDContext] = MergeTopologies()
    jobs.append(job0)

    pipe: Pipeline[MDContext] = Pipeline(*jobs)
    pipe(context)

    log.info("OK")

    next_step(context)


def topology_save_routine(context: MDContext, next_step: NextStep) -> None:
    log = logging.getLogger(topology_save_routine.__name__)
    log.info(" Starting save topology routine.")

    jobs: list[PipeStep[MDContext]] = []

    job1: PipeStep[MDContext] = SetCurrentTopology(
        context.simulation_menager.topol_configs[-1]
    )

    job2: PipeStep[MDContext] = SetCurrentRun(context.simulation_menager.run_configs[0])
    job3: PipeStep[MDContext] = ReadCoordinates()

    job4: PipeStep[MDContext] = WriteParameters()
    job5: PipeStep[MDContext] = WriteCoordinates()

    jobs.extend([job1, job2, job3, job4, job5])

    pipe: Pipeline[MDContext] = Pipeline(*jobs)
    pipe(context)

    log.info("OK")

    next_step(context)


def topology_save_routine_repeating_scenario(
    context: MDContext, next_step: NextStep
) -> None:
    log = logging.getLogger(topology_save_routine.__name__)
    log.info(" Starting save topology routine.")

    jobs: list[PipeStep[MDContext]] = []

    job1: PipeStep[MDContext] = SetCurrentTopology(
        context.simulation_menager.topol_configs[-1]
    )

    pipe: Pipeline[MDContext] = Pipeline(job1)
    pipe(context)

    for runmd in context.simulation_menager.run_configs:
        job2: PipeStep[MDContext] = SetCurrentRun(runmd)
        job3: PipeStep[MDContext] = ReadCoordinates()
        job4: PipeStep[MDContext] = WriteCoordinates(index=runmd["INDEX"])

        pipe: Pipeline[MDContext] = Pipeline(job2, job3, job4)
        pipe(context)

    pipe: Pipeline[MDContext] = Pipeline(
        SetCurrentRun(context.simulation_menager.run_configs[0]), WriteParameters()
    )
    pipe(context)

    log.info("OK")

    next_step(context)


def run_routine(context: MDContext, next_step: NextStep) -> None:
    log = logging.getLogger(run_routine.__name__)
    log.info(" Starting run routine.")

    if Path(context.environment_menager.data_dir / "md.run").exists():
        context.environment_menager.make_duplicate(
            context.environment_menager.data_dir / "md.run"
        )

    runMD_configs: list[RunConfig] = context.simulation_menager.run_configs
    jobs: list[PipeStep[MDContext]] = []

    for run_config in runMD_configs:
        jobs.append(SetCurrentRun(run_config))
        jobs.append(PrepareMDOptions())

        for i in range(run_config["NRUNS"]):
            jobs.append(RunMD(i, rerun=False))

    if context.CONNECTION:
        if Path(context.environment_menager.data_dir / "md.slurm").exists():
            context.environment_menager.make_duplicate(
                context.environment_menager.data_dir / "md.slurm"
            )

        jobs.append(RunSLURM())

    # RUNNING PIPELINE #
    pipe: Pipeline[MDContext] = Pipeline(*jobs)
    pipe(context)

    log.info("OK")

    next_step(context)


def rerun_routine(context: MDContext, next_step: NextStep) -> None:
    log = logging.getLogger(rerun_routine.__name__)
    log.info(" Starting rerun routine.")

    unfinished = context.find_unfinished()
    if unfinished.empty:
        log.info("All run has been finished successfuly.")
        exit()

    if Path(context.environment_menager.data_dir / "md.run").exists():
        context.environment_menager.make_duplicate(
            context.environment_menager.data_dir / "md.run"
        )

    jobs: list[PipeStep[MDContext]] = []
    run: Series[str]
    for _, run in unfinished.iterrows():
        sim_name: str = run["SIMULATION NAME"]
        number, *_ = sim_name.split("-")
        jobs.append(
            FindRunConfig(
                run["COORDINATE FILE"],
                run["CONFIG FILE"],
            )
        )
        jobs.append(FindTopolConfig(run["TOPOLOGY FILE"]))

        jobs.append(RunMD(int(number), rerun=True))

    jobs.append(PrepareMDOptions())

    if context.CONNECTION:
        if Path(context.environment_menager.data_dir / "md.slurm").exists():
            context.environment_menager.make_duplicate(
                context.environment_menager.data_dir / "md.slurm"
            )
        jobs.append(RunSLURM())

    # RUNNING PIPELINE #
    pipe: Pipeline[MDContext] = Pipeline(*jobs)
    pipe(context)

    log.info("OK")

    next_step(context)


def check_runs_routine(context: MDContext, next_step: NextStep) -> None:
    log = logging.getLogger(check_runs_routine.__name__)
    log.info(" Starting check run routine.")

    if not context.DATABASE["DATABASE"]:
        raise ValueError("Database has not been set.")

    def filter_logs(file_name: Path) -> bool:
        ext = file_name.suffix
        if ext in [".log", ".mdout"]:
            return True
        return False

    log_list = list(
        filter(filter_logs, list(context.environment_menager.data_dir.iterdir()))
    )
    log.debug(f"Found logs: {' '.join([i.name for i in log_list])}")

    jobs: list[PipeStep[MDContext]] = []
    for log_file in log_list:
        jobs.append(CheckProgress(log_file))

    pipe: Pipeline[MDContext] = Pipeline(*jobs)
    pipe(context)

    search_result = context.DATABASE["DATABASE"].find_entries(
        {"PROJECT NAME": context.ENVIRONMENT["PROJECT_NAME"]}
    )
    log.debug(search_result)
    log.debug(search_result[["SIMULATION NAME", "STAGE", "PID"]])

    log.info("OK")

    next_step(context)


def find_pid(context: MDContext, next_step: NextStep) -> None:
    log = logging.getLogger(watch_queue_routine.__name__)
    log.info(" Finding PID.")

    if not context.DATABASE["DATABASE"]:
        raise ValueError("Database has not been set.")

    if not context.SLURM["SSH_CONNECTION"]:
        raise ValueError("SSH has not been configured.")

    if not context.CONNECTION:
        raise ValueError("SHH connection has not been set.")

    log.debug(f"Current PID {context.SLURM['PID']}")
    log.info("Getting PID from database.")
    search_result = context.database_menager.database.find_entries(
        {"PROJECT NAME": context.ENVIRONMENT["PROJECT_NAME"]}
    )
    log.debug(search_result[["PROJECT NAME", "SIMULATION NAME", "STAGE", "PID"]])
    pid: str = search_result["PID"].max()
    try:
        context.SLURM["PID"] = int(pid)
    except ValueError:
        log.warning(
            f"Invalid PID in database {context.SLURM['PID']}. Reading PID from slurm log files."
        )
        process = context.SLURM["SSH_CONNECTION"].run_remotely(
            f"grep -m 1 {context.ENVIRONMENT['PROJECT_NAME']}"
            + r" /home/mszatko/slurm-* | grep -oP '(?<=slurm-)\d+'"
        )
        log.debug(f"Return code: {process.returncode}")
        if process.returncode != 0:
            log.error(f"Running failed to find pid from slurm logs!")
            exit()
        context.SLURM["PID"] = process.stdout.strip().split("\n")[-1]

    context.modify_entry(
        ("PID", str(context.SLURM["PID"])),
        {"PROJECT NAME": context.ENVIRONMENT["PROJECT_NAME"]},
    )

    next_step(context)


def watch_queue_routine(context: MDContext, next_step: NextStep) -> None:
    pipe: Pipeline[MDContext]

    def parse_data(data: str) -> str | None:
        lines = data.strip().split("\n")
        for i, line in enumerate(lines):
            if i == 0:
                continue
            _, state, _, _ = line.split("|")
            if i == 2:
                return state
        return None

    if not context.DATABASE["DATABASE"]:
        raise ValueError("Database has not been set.")

    if not context.SLURM["SSH_CONNECTION"]:
        raise ValueError("SSH has not been configured.")

    if not context.CONNECTION:
        raise ValueError("SHH connection has not been set.")

    log = logging.getLogger(watch_queue_routine.__name__)
    log.info(" Starting watching queue routine.")

    if context.SLURM["PID"] is None:
        pipe = Pipeline(find_pid)
        pipe(context)

    log.info(f"Selected PID {context.SLURM['PID']}")
    if context.SLURM["PID"] is None or context.SLURM["PID"] is numpy.nan:
        raise ValueError("No run has been found.")

    pid = context.SLURM["PID"]
    while True:
        process = context.SLURM["SSH_CONNECTION"].run_remotely(
            f"/opt/slurm/current/bin/sacct --jobs={pid} -p -b"
        )

        log.debug(f"Return code: {process.returncode}")
        if process.returncode != 0:
            log.error(f"Running 'sacct --jobs={pid} -b -p' has failed.")
            break

        status: str | None = parse_data(process.stdout)

        if status is None:
            log.error(f"There was a problem in parsing the output of sacct.")
            log.debug(process.stdout)
            break
        if status != "RUNNING" and status != "PENDING":
            log.error(f"The job has ended! Status: {status}")
            pipe = Pipeline(
                download_logs,
                check_runs_routine,
                # download_finished,
            )
            pipe(context)
            break

        pipe = Pipeline(
            download_logs,
            check_runs_routine,
        )
        pipe(context)

        current_time_seconds = time.time()

        new_time_seconds = current_time_seconds + 15 * 60
        new_time_struct = time.localtime(new_time_seconds)
        new_time_str = time.strftime("%Y-%m-%d %H:%M:%S", new_time_struct)

        log.info(f"Next check: {new_time_str}")
        time.sleep(900)

    next_step(context)


def remote_run_routine(context: MDContext, next_step: NextStep) -> None:
    log = logging.getLogger(remote_run_routine.__name__)
    log.info(" Starting remote run routine.")

    if not context.DATABASE["DATABASE"]:
        raise ValueError("Database has not been set.")

    if not context.SLURM["SSH_CONNECTION"]:
        raise ValueError("SSH has not been configured.")

    if not context.CONNECTION:
        raise ValueError("SHH connection has not been set.")

    data_dir: str = str(context.ENVIRONMENT["DATA_DIR"])
    config_files: list[str] = [
        str(i["CONFIG_FILE"].name) for i in context.simulation_menager.run_configs
    ]
    cooord_files: list[str] = [
        str(i["START_COORDINATES_FILE"].name)
        for i in context.simulation_menager.run_configs
    ]
    basename: str = str(context.ENVIRONMENT["BASENAME"])
    project_name: str = context.ENVIRONMENT["PROJECT_NAME"]
    slurm_file: str = "md.slurm"
    tar_file = f"{basename}.tar"
    remote_dir: str = str(context.SLURM["REMOTE_DIR"])
    remote_adress: str = context.SLURM["REMOTE_ADRESS"]
    remote_data_dir: str = f"{remote_dir}/{project_name}"

    to_send = (
        f"{basename}.top",
        *cooord_files,
        *config_files,
        slurm_file,
    )

    _ = context.SLURM["SSH_CONNECTION"].run_remotely(
        f"mkdir -p {remote_data_dir}",
    )

    _ = context.SLURM["SSH_CONNECTION"].run_locally(
        [
            "tar",
            "cfv",
            f"{data_dir}/{tar_file}",
            "-C",
            data_dir,
            *to_send,
        ]
    )

    context.SLURM["SSH_CONNECTION"].send_files(
        f"{data_dir}/{tar_file}",
        f"{remote_adress}:{remote_data_dir}",
    )

    _ = context.SLURM["SSH_CONNECTION"].run_remotely(
        f"tar xfv {remote_data_dir}/{tar_file} -C {remote_data_dir}"
    )

    process = context.SLURM["SSH_CONNECTION"].run_remotely(
        f"/opt/slurm/current/bin/sbatch {remote_data_dir}/md.slurm"
    )

    pid = process.stdout.split()[3]
    log.info(f"PID {pid}")
    context.SLURM["PID"] = pid
    context.modify_entry(("PID", pid), {"PROJECT NAME": project_name})

    search_result = context.database_menager.database.find_entries(
        {"PROJECT NAME": context.ENVIRONMENT["PROJECT_NAME"]}
    )
    log.debug(search_result[["PROJECT NAME", "SIMULATION NAME", "STAGE", "PID"]])

    next_step(context)


def download_logs(context: MDContext, next_step: NextStep) -> None:
    log = logging.getLogger(download_logs.__name__)
    log.info("Downloading logs.")

    if not context.SLURM["SSH_CONNECTION"]:
        raise ValueError("SSH has not been configured.")

    if not context.CONNECTION:
        raise ValueError("SHH connection has not been set.")

    project_name: str = context.ENVIRONMENT["PROJECT_NAME"]
    data_dir: str = str(context.environment_menager.data_dir)
    remote_dir: str = str(context.SLURM["REMOTE_DIR"])
    remote_adress: str = context.SLURM["REMOTE_ADRESS"]
    remote_data_dir: str = f"{remote_dir}/{project_name}"

    log_ext: str = ""
    if context.ENVIRONMENT["SOFTWARE"] == "amber":
        log_ext = "mdout"
    if context.ENVIRONMENT["SOFTWARE"] == "gromacs":
        log_ext = "log"

    if log_ext == "":
        raise ValueError("Software has not been recognized.")

    context.SLURM["SSH_CONNECTION"].send_files(
        f"{remote_adress}:{remote_data_dir}/*.{log_ext}", data_dir
    )
    next_step(context)


def download_finished(context: MDContext, next_step: NextStep) -> None:
    log = logging.getLogger(download_finished.__name__)
    log.info("Downloading finished.")

    if not context.DATABASE["DATABASE"]:
        raise ValueError("Database has not been set.")

    if not context.SLURM["SSH_CONNECTION"]:
        raise ValueError("SSH has not been configured.")

    if not context.CONNECTION:
        raise ValueError("SHH connection has not been set.")

    project_name: str = context.ENVIRONMENT["PROJECT_NAME"]
    download_dir: str = str(context.SLURM["DOWNLOAD_DIR"])
    lustre_dir: str = str(context.SLURM["LUSTRE_DIR"])
    remote_adress: str = context.SLURM["REMOTE_ADRESS"]
    lustra_data_dir: str = f"{lustre_dir}/{project_name}"

    search_result = context.database_menager.database.find_entries(
        {"PROJECT NAME": context.ENVIRONMENT["PROJECT_NAME"]}
    )
    log.debug(search_result)
    log.debug(search_result[["SIMULATION NAME", "STAGE", "PID"]])

    if search_result.empty:
        raise ValueError("No such runs has been found.")

    simulation_names: list[str] = []

    for _, entry in search_result.iterrows():
        if entry["STAGE"] == "Finished":
            simulation_names.append(entry["SIMULATION NAME"])

    files = "{%s}.sim.tar" % (",".join(simulation_names))

    context.SLURM["SSH_CONNECTION"].send_files(
        f"{remote_adress}:{lustra_data_dir}/" + files,
        download_dir,
    )

    for _, entry in search_result.iterrows():
        if entry["STAGE"] == "Finished":
            context.modify_entry(
                ("STAGE", "DOWNLOADED"),
                {
                    "SIMULATION NAME": entry["SIMULATION NAME"],
                    "PROJECT NAME": entry["PROJECT NAME"],
                },
            )

    for sim_name in simulation_names:
        _ = context.SLURM["SSH_CONNECTION"].run_locally(
            ["tar", "xfv", f"{download_dir}/{sim_name}.sim.tar", "-C", download_dir]
        )
        _ = context.SLURM["SSH_CONNECTION"].run_locally(
            ["rm", f"{download_dir}/{sim_name}.sim.tar"]
        )

    # context.SLURM["SSH_CONNECTION"].run_remotely(
    #     f"rm {context.SLURM['REMOTE_DIR'] / context.ENVIRONMENT['PROJECT_NAME']}/{files}"
    # )

    next_step(context)


if __name__ == "__main__":
    import sys

    # Set a new recursion depth limit
    sys.setrecursionlimit(2000)  # Set to a higher value as per your needs
    eq_list: list[str] = [
        "slurm-eq-boc-csr4.config",
        "slurm-eq-boc-cssrr4.config",
        "slurm-eq-boc-dsr4.config",
        "slurm-eq-boc-lsr4.config",
        "slurm-eq-boc-pasr4.config",
        "slurm-eq-boc-pgsr4.config",
        "slurm-eq-boc-pssrr4.config",
        "slurm-eq-boc-vsr4.config",
    ]

    sa_list: list[str] = [
        # "slurm-sa-boc-a4.config",
        # "slurm-sa-boc-csr4.config",
        # "slurm-sa-boc-cssrr4.config",
        # "slurm-sa-boc-dsr4.config",
        # "slurm-sa-boc-lsr4.config",
        # "slurm-sa-boc-pasr4.config",
        # "slurm-sa-boc-pgsr4.config",
        # "slurm-sa-boc-pssrr4.config",
        # "slurm-sa-boc-vsr4.config",
    ]

    classic_list: list[str] = [
        "slurm-classic-boc-a4.config",
        "slurm-classic-boc-csr4.config",
        "slurm-classic-boc-cssrr4.config",
        "slurm-classic-boc-dsr4.config",
        "slurm-classic-boc-lsr4.config",
        "slurm-classic-boc-pasr4.config",
        "slurm-classic-boc-pgsr4.config",
        "slurm-classic-boc-pssrr4.config",
        "slurm-classic-boc-vsr4.config",
    ]

    for i, config in enumerate(classic_list):
        root = "/home/keppen/MD/side_chains/configs/" + config
        # root = "/home/keppen/MD/side_chains/configs/testing.config"

        context_config = Path(f"{root}")

        test_context = MDContext.from_config(context_config)

        unfinished = test_context.find_unfinished()
        print(unfinished["PID"])
        if unfinished.empty:
            print("--- JOB COMPLETED AND DOWNLOADED! ---")
            continue

        print("TIME option will be overriden to '24:0:0!")
        test_context.SLURM["TIME"] = "44:0:0"
        test_context.SLURM["DOWNLOAD_DIR"] = Path("XXX")

        pipe: Pipeline[MDContext] = Pipeline(
            topology_setup_routine,
            # topology_save_routine,
            topology_save_routine_repeating_scenario,
            # run_routine,
            # rerun_routine,
            # remote_run_routine,
            # check_runs_routine,
            watch_queue_routine,
        )
        pipe(test_context)

        dataset = test_context.DATABASE["DATABASE"].find_entries(
            {"PROJECT NAME": test_context.ENVIRONMENT["PROJECT_NAME"]}
        )

        test_context.database_menager.database.save()
        print(dataset[["PROJECT NAME", "STAGE", "PID"]])

        # exit()
