from prefect import flow, task
from prefect_shell import ShellOperation


@task
def hours_long_sleep_task(hours=2):
    """Simulate a process that runs for hours - simple sleep"""
    seconds = hours * 3600
    with ShellOperation(commands=[f"sleep {seconds}"]) as sleep_operation:
        sleep_process = sleep_operation.trigger()
        sleep_process.wait_for_completion()
        # result = sleep_process.result()


@task
def run_nextflow(working_dir):
    """Run the next flow after the sleep operation"""
    commands = ["nextflow run main.nf"]
    with ShellOperation(commands=commands, working_dir=working_dir) as nextflow_operation:
        nextflow_process = nextflow_operation.trigger()
        nextflow_process.wait_for_completion()
        result = nextflow_process.result()


@flow
def hours_long_test_flow():
    """Test 2-hour sleep operation"""
    print("Starting 2-hour sleep test...")

    sleep_result = hours_long_sleep_task(hours=2)
    print(f"2-hour sleep completed: {sleep_result.return_code}")


if __name__ == "__main__":
    # Deploy the flow to the specified work pool
    flow.from_source(
        source="git@github.com:simalicrum/shell-test.git",
        entrypoint="shell_test.py:hours_long_test_flow",
    ).deploy(name="hours_long_test_flow", work_pool_name="hpc1-pool", work_queue_name="default")
