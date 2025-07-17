import asyncio
import subprocess

from prefect import flow, task
from prefect.logging import get_run_logger


@task
async def hours_long_sleep_task(hours=2):
    """Simulate a process that runs for hours - simple sleep"""
    seconds = hours * 3600
    # Use subprocess to run the sleep command
    command = f"setsid bash -c 'sleep {seconds}'"

    process = await asyncio.create_subprocess_shell(
        command, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )

    stdout, stderr = await process.communicate()
    return process.returncode


@task
async def run_nextflow(working_dir):
    """Run the next flow after the sleep operation"""
    logger = get_run_logger()
    logger.info("Starting Nextflow operation...")

    command = (
        "exec /shared/mondrian/nextflow -q run https://github.com/molonc/mondrian_nf "
        "-r v0.1.8 "
        "-params-file params.yaml "
        "-profile singularity,slurm "
        "-with-report report.html "
        "-with-timeline timeline.html "
        "-resume "
        "-ansi-log false"
    )

    # Create subprocess and run in the specified working directory
    process = await asyncio.create_subprocess_shell(
        command, cwd=working_dir, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )

    logger.info(f"Nextflow process PID: {process.pid}")

    # Monitor the process while it's running
    while process.returncode is None:
        logger.info(f"Nextflow process PID: {process.pid}")
        logger.info(f"Nextflow process return code: {process.returncode}")
        await asyncio.sleep(5)

        # Check if process has finished
        if process.returncode is not None:
            break

    stdout, stderr = await process.communicate()
    logger.info(f"Nextflow process completed with return code: {process.returncode}")

    if stdout:
        logger.info(f"STDOUT: {stdout.decode()}")
    if stderr:
        logger.error(f"STDERR: {stderr.decode()}")

    return process.returncode


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
