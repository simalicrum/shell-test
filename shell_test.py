from prefect import flow, task
from prefect_shell import ShellOperation


@task(
    on_failure=[
        lambda task, task_run, state,: print(f"Task {task.name} failed with state: {state}"),
    ],
    retries=3,
    retry_delay_seconds=10,
)
async def hours_long_sleep_task(hours=2):
    """Simulate a process that runs for hours - simple sleep"""
    seconds = hours * 3600
    with ShellOperation(commands=[f"sleep {seconds}"]) as sleep_operation:
        sleep_process = await sleep_operation.trigger()
        await sleep_process.wait_for_completion()
        result = sleep_process.result()


@flow
async def hours_long_test_flow():
    """Test 2-hour sleep operation"""
    print("Starting 2-hour sleep test...")

    sleep_result = await hours_long_sleep_task(hours=2)
    print(f"2-hour sleep completed: {sleep_result.return_code}")


if __name__ == "__main__":
    # Deploy the flow to the specified work pool
    flow.from_source(
        source="git@github.com:simalicrum/shell-test.git",
        entrypoint="shell_test.py:hours_long_test_flow",
    ).deploy(name="hours_long_test_flow", work_pool_name="hpc1-pool", work_queue_name="default")
