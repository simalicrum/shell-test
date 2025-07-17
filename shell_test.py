import asyncio
import os
import signal
import subprocess

from prefect import flow, task
from prefect.logging import get_run_logger
from prefect.runtime.task_run import TaskRunContext
from prefect_shell import ShellOperation


@task
async def hours_long_sleep_task(hours=2):
    """Simulate a process that runs for hours - simple sleep"""
    seconds = hours * 3600
    # Use setsid to create a new process group, so all children are killed together
    command = f"setsid bash -c 'sleep {seconds}'"

    with ShellOperation(commands=[command]) as sleep_operation:
        sleep_process = await sleep_operation.trigger()
        await sleep_process.wait_for_completion()


@task
async def run_nextflow(version: str):
    log = get_run_logger()

    # 1️⃣ Spawn in new process group
    proc = subprocess.Popen(
        [
            "/shared/mondrian/nextflow",
            "-q",
            "run",
            "https://github.com/molonc/mondrian_nf",
            "-r",
            version,
        ],
        start_new_session=True,  # key!
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        pgid = os.getpgid(proc.pid)
    except AttributeError:
        pgid = proc.pid
    log.info(f"Started Nextflow PID={proc.pid} PGID={pgid}")

    # 2️⃣ Attach cancel callback
    ctx = TaskRunContext.get()
    ctx.cancel_scope.add_cancel_callback(lambda *_: _kill_pgid(pgid, log))

    # 3️⃣ Stream logs and wait
    async for line in _async_lines(proc):
        log.info(line.strip())

    rc = await asyncio.to_thread(proc.wait)
    if rc != 0:
        raise RuntimeError(f"Nextflow exited with {rc}")


def _kill_pgid(pgid, log):
    try:
        log.warning(f"Cancelling... sending SIGTERM to PGID {pgid}")
        os.killpg(pgid, signal.SIGTERM)
        asyncio.run(asyncio.sleep(15))
        os.killpg(pgid, signal.SIGKILL)
    except ProcessLookupError:
        pass


async def _async_lines(proc):
    loop = asyncio.get_running_loop()
    while True:
        line = await loop.run_in_executor(None, proc.stdout.readline)
        if not line:
            break
        yield line


@flow
async def hours_long_test_flow(working_dir):
    # """Test 2-hour sleep operation"""
    # print("Starting 2-hour sleep test...")

    # sleep_result = hours_long_sleep_task(hours=2)
    # print(f"2-hour sleep completed: {sleep_result.return_code}")
    await run_nextflow(working_dir=working_dir)


if __name__ == "__main__":
    # Deploy the flow to the specified work pool
    flow.from_source(
        source="git@github.com:simalicrum/shell-test.git",
        entrypoint="shell_test.py:hours_long_test_flow",
    ).deploy(name="hours_long_test_flow", work_pool_name="hpc1-pool", work_queue_name="default")
