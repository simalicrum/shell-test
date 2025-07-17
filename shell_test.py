from prefect import flow, task
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
async def run_nextflow(working_dir):
    """Run the next flow after the sleep operation"""
    # Wrap the nextflow command in setsid to create a new process group
    # This ensures all child processes are killed when the parent is terminated
    commands = [
        (
            "exec /shared/mondrian/nextflow -q run https://github.com/molonc/mondrian_nf "
            "-r v0.1.8 "
            "-params-file params.yaml "
            "-profile singularity,slurm "
            "-with-report report.html "
            "-with-timeline timeline.html "
            "-resume "
            "-ansi-log false"
        )
    ]

    with ShellOperation(commands=commands, working_dir=working_dir) as nextflow_operation:
        nextflow_process = await nextflow_operation.trigger()
        await nextflow_process.wait_for_completion()


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
