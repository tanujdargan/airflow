# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import datetime
import os
import re
import sys
import textwrap
import time
from abc import ABCMeta, abstractmethod
from collections.abc import Generator
from contextlib import contextmanager
from enum import Enum
from multiprocessing.pool import ApplyResult, Pool
from pathlib import Path
from tempfile import NamedTemporaryFile
from threading import Thread
from typing import Any, NamedTuple

from rich.table import Table

from airflow_breeze.utils.console import MessageType, Output, get_console
from airflow_breeze.utils.shared_options import (
    get_dry_run,
    get_forced_answer,
    get_verbose,
    set_dry_run,
    set_forced_answer,
    set_verbose,
)

MAX_LINE_LENGTH = 155


def _init_worker(verbose: bool, dry_run: bool, forced_answer: str | None):
    set_verbose(verbose)
    set_dry_run(dry_run)
    set_forced_answer(forced_answer)


def create_pool(parallelism: int) -> Pool:
    return Pool(
        parallelism, initializer=_init_worker, initargs=(get_verbose(), get_dry_run(), get_forced_answer())
    )


def get_temp_file_name() -> str:
    file = NamedTemporaryFile(mode="w+t", delete=False, prefix="parallel")
    name = file.name
    file.close()
    return name


def get_output_files(titles: list[str]) -> list[Output]:
    outputs = [Output(title=title, file_name=get_temp_file_name()) for title in titles]
    for out in outputs:
        get_console().print(f"[info]Capturing output of {out.escaped_title}:[/] {out.file_name}")
    return outputs


def nice_timedelta(delta: datetime.timedelta):
    d = {"d": delta.days}
    d["h"], rem = divmod(delta.seconds, 3600)
    d["m"], d["s"] = divmod(rem, 60)
    return "{d} days {h:02}:{m:02}:{s:02}".format(**d) if d["d"] else "{h:02}:{m:02}:{s:02}".format(**d)


ANSI_COLOUR_MATCHER = re.compile(r"(?:\x1B[@-_]|[\x80-\x9F])[0-?]*[ -/]*[@-~]")


def remove_ansi_colours(line: str):
    return ANSI_COLOUR_MATCHER.sub("", line)


def get_last_lines_of_file(file_name: str, num_lines: int = 2) -> tuple[list[str], list[str]]:
    """
    Get last lines of a file efficiently, without reading the whole file (with some limitations).
    Assumptions ara that line length not bigger than ~180 chars.

    :param file_name: name of the file
    :param num_lines: number of lines to return (max)
    :return: Tuple - last lines of the file in two variants: original and with removed ansi colours
    """
    # account for EOL
    max_read = (180 + 2) * num_lines
    try:
        seek_size = min(os.stat(file_name).st_size, max_read)
    except FileNotFoundError:
        return [], []
    with open(file_name, "rb") as temp_f:
        temp_f.seek(-seek_size, os.SEEK_END)
        tail = temp_f.read().decode(errors="ignore")
    last_lines = tail.splitlines()[-num_lines:]
    last_lines_no_colors = [remove_ansi_colours(line) for line in last_lines]
    return last_lines, last_lines_no_colors


class AbstractProgressInfoMatcher(metaclass=ABCMeta):
    @abstractmethod
    def get_best_matching_lines(self, output: Output) -> list[str] | None:
        """
        Return best matching lines of the output.
        :return: array of lines to print
        """


class ShowLastLineProgressMatcher(AbstractProgressInfoMatcher):
    def get_best_matching_lines(self, output: Output) -> list[str] | None:
        last_lines, _ = get_last_lines_of_file(output.file_name, num_lines=1)
        return last_lines


class DockerBuildxProgressMatcher(AbstractProgressInfoMatcher):
    DOCKER_BUILDX_PROGRESS_MATCHER = re.compile(r"\s*#(\d*) ")

    def __init__(self):
        self.last_docker_build_lines: dict[str, str] = {}

    def get_best_matching_lines(self, output: Output) -> list[str] | None:
        last_lines, last_lines_no_colors = get_last_lines_of_file(output.file_name, num_lines=5)
        best_progress: int = 0
        best_line: str | None = None
        for index, line in enumerate(last_lines_no_colors):
            match = DockerBuildxProgressMatcher.DOCKER_BUILDX_PROGRESS_MATCHER.match(line)
            if match:
                docker_progress = int(match.group(1))
                if docker_progress > best_progress:
                    best_progress = docker_progress
                    best_line = last_lines[index]
        if best_line is None:
            best_line = self.last_docker_build_lines.get(output.file_name)
        else:
            self.last_docker_build_lines[output.file_name] = best_line
        if best_line is None:
            return None
        return [best_line]


class GenericRegexpProgressMatcher(AbstractProgressInfoMatcher):
    """
    Matches lines from the output based on regular expressions:

    :param regexp: regular expression matching lines that should be displayed
    :param regexp_for_joined_line: optional regular expression for lines that might be shown together with the
           following matching lines. Useful, when progress status is only visible in previous line, for
           example when you have test output like that, you want to show both lines:

               test1 ....... [ 50%]
               test2 ...
    """

    def __init__(
        self,
        regexp: str,
        lines_to_search: int,
        regexp_for_joined_line: str | None = None,
    ):
        self.last_good_match: dict[str, str] = {}
        self.matcher = re.compile(regexp)
        self.lines_to_search = lines_to_search
        self.matcher_for_joined_line = re.compile(regexp_for_joined_line) if regexp_for_joined_line else None

    def get_best_matching_lines(self, output: Output) -> list[str] | None:
        last_lines, last_lines_no_colors = get_last_lines_of_file(
            output.file_name, num_lines=self.lines_to_search
        )
        best_line: str | None = None
        previous_line: str | None = None
        for index, line in enumerate(last_lines_no_colors):
            match = self.matcher.match(line)
            if match:
                best_line = last_lines[index]
                if self.matcher_for_joined_line is not None and index > 0:
                    if self.matcher_for_joined_line.match(last_lines_no_colors[index - 1]):
                        previous_line = last_lines[index - 1].strip()
        if best_line is not None:
            if self.matcher_for_joined_line is not None and previous_line is not None:
                list_to_return: list[str] = [previous_line, best_line]
                return list_to_return
            self.last_good_match[output.file_name] = best_line
        last_match = self.last_good_match.get(output.file_name)
        if last_match is None:
            return None
        return [last_match]


DOCKER_PULL_PROGRESS_REGEXP = r"^[0-9a-f]+: .*|.*\[[ \d%]*\].*|^Waiting"


def bytes2human(n):
    symbols = ("K", "M", "G", "T", "P", "E", "Z", "Y")
    prefix = {}
    for i, s in enumerate(symbols):
        prefix[s] = 1 << (i + 1) * 10
    for s in reversed(symbols):
        if n >= prefix[s]:
            value = n / prefix[s]
            return f"{value:.1f}{s}"
    return f"{n}B"


def get_printable_value(key: str, value: Any) -> str:
    if key == "percent":
        return f"{value} %"
    if isinstance(value, (int, float)):
        return bytes2human(value)
    return str(value)


def get_single_tuple_array(title: str, t: NamedTuple) -> Table:
    table = Table(title=title)
    row = []
    for key, value in t._asdict().items():
        table.add_column(header=key, header_style="info")
        row.append(get_printable_value(key, value))
    table.add_row(*row, style="special")
    return table


def get_multi_tuple_array(title: str, tuples: list[tuple[NamedTuple, ...]]) -> Table:
    table = Table(title=title)
    first_tuple = tuples[0]
    keys: list[str] = []
    for named_tuple in first_tuple:
        keys.extend(named_tuple._asdict().keys())
    for key in keys:
        table.add_column(header=key, header_style="info")
    for t in tuples:
        row = []
        for named_tuple in t:
            for key, value in named_tuple._asdict().items():
                row.append(get_printable_value(key, value))
        table.add_row(*row, style="special")
    return table


IGNORED_FSTYPES = [
    "autofs",
    "bps",
    "cgroup",
    "cgroup2",
    "configfs",
    "debugfs",
    "devpts",
    "fusectl",
    "mqueue",
    "nsfs",
    "overlay",
    "proc",
    "pstore",
    "squashfs",
    "tracefs",
]


class ParallelMonitor(Thread):
    def __init__(
        self,
        outputs: list[Output],
        initial_time_in_seconds: int = 2,
        time_in_seconds: int = int(os.environ.get("AIRFLOW_MONITOR_DELAY_TIME_IN_SECONDS", "20")),
        debug_resources: bool = False,
        progress_matcher: AbstractProgressInfoMatcher | None = None,
    ):
        super().__init__(daemon=True)
        self.outputs = outputs
        self.initial_time_in_seconds = initial_time_in_seconds
        self.time_in_seconds = time_in_seconds
        self.debug_resources = debug_resources
        self.progress_matcher = progress_matcher
        self.start_time = datetime.datetime.now(tz=datetime.timezone.utc)

    def print_single_progress(self, output: Output):
        if self.progress_matcher:
            progress_lines: list[str] | None = self.progress_matcher.get_best_matching_lines(output)
            if progress_lines is not None:
                first_line = True
                for index, line in enumerate(progress_lines):
                    if len(remove_ansi_colours(line)) > MAX_LINE_LENGTH:
                        # This is a bit cheating - the line will be much shorter in case it contains colors
                        # Also we need to clear color just in case color reset is removed by textwrap.shorten
                        current_line = textwrap.shorten(progress_lines[index], MAX_LINE_LENGTH) + "\033[0;0m"
                    else:
                        current_line = progress_lines[index]
                    if current_line:
                        prefix = f"Progress: {output.title:<30}"
                        if not first_line:
                            # remove job prefix after first line
                            prefix = " " * len(prefix)
                        print(f"{prefix}{current_line}\033[0;0m")
                        first_line = False
            else:
                size = os.path.getsize(output.file_name) if Path(output.file_name).exists() else 0
                default_output = f"File: {output.file_name} Size: {size:>10} bytes"
                get_console().print(f"Progress: {output.escaped_title[:30]:<30} {default_output:>161}")

    def print_summary(self):
        import psutil

        time_passed = datetime.datetime.now(tz=datetime.timezone.utc) - self.start_time
        get_console().rule()
        for output in self.outputs:
            self.print_single_progress(output)
        get_console().rule(title=f"Time passed: {nice_timedelta(time_passed)}")
        if self.debug_resources:
            get_console().print(get_single_tuple_array("Virtual memory", psutil.virtual_memory()))
            disk_stats = []
            for partition in psutil.disk_partitions(all=True):
                if partition.fstype not in IGNORED_FSTYPES:
                    try:
                        disk_stats.append((partition, psutil.disk_usage(partition.mountpoint)))
                    except Exception:
                        get_console().print(f"No disk usage info for {partition.mountpoint}")
            get_console().print(get_multi_tuple_array("Disk usage", disk_stats))
            # Print CPU percent usage
            get_console().print("CPU usage:")
            usage = psutil.cpu_percent(percpu=True, interval=None)
            for i, cpu_usage in enumerate(usage):
                get_console().print(f"CPU {i}: {cpu_usage / 100:.0%}")

    def run(self):
        try:
            time.sleep(self.initial_time_in_seconds)
            while True:
                self.print_summary()
                time.sleep(self.time_in_seconds)
        except Exception:
            get_console().print_exception(show_locals=True)


def print_async_result_status(completed_list: list[ApplyResult]) -> None:
    """
    Print status of completed async results.
    :param completed_list: list of completed async results.
    """
    completed_list.sort(key=lambda x: x.get()[1])
    get_console().print()
    for result in completed_list:
        return_code, info = result.get()
        info = info.replace("[", "\\[")
        if return_code != 0:
            get_console().print(f"[error]NOK[/] for {info}: Return code: {return_code}.")
        else:
            get_console().print(f"[success]OK [/] for {info}.")
    get_console().print()


def get_completed_result_list(results: list[ApplyResult]) -> list[ApplyResult]:
    """Return completed results from the list."""
    return [result for result in results if result.ready()]


class SummarizeAfter(Enum):
    NO_SUMMARY = 0
    FAILURE = 1
    SUCCESS = 2
    BOTH = 3


def check_async_run_results(
    results: list[ApplyResult],
    success_message: str,
    outputs: list[Output],
    include_success_outputs: bool,
    poll_time_seconds: float = 0.2,
    skip_cleanup: bool = False,
    summarize_on_ci: SummarizeAfter = SummarizeAfter.NO_SUMMARY,
    summary_start_regexp: str | None = None,
    terminated_on_timeout: bool = False,
):
    """
    Check if all async results were success.

    Exits with error if:

    * exit code 1: some tasks failed
    * exit code 2: some tasks were terminated on timeout

    :param results: results of parallel runs (expected in the form of Tuple: (return_code, info)
    :param outputs: outputs where results are written to
    :param success_message: Success string printed when everything is OK
    :param include_success_outputs: include outputs of successful parallel runs
    :param poll_time_seconds: what's the poll time between checks
    :param skip_cleanup: whether to skip cleanup of temporary files.
    :param summarize_on_ci: determines when to summarize the parallel jobs  when they are completed in CI,
        outside the folded CI output
    :param summary_start_regexp: the regexp that determines line after which
        outputs should be printed as summary, so that you do not have to look at the folded details of
        the run in CI
    :param terminated_on_timeout: whether the run was terminated on timeout
    """
    if terminated_on_timeout:
        print_outputs_on_timeout(outputs, results, include_success_outputs)
        sys.exit(2)
    completed_list = wait_for_all_tasks_completed(poll_time_seconds, results)
    print_async_result_status(completed_list)
    print_logs_on_completion(include_success_outputs, outputs, results)
    summarize_results_outside_of_folded_logs(outputs, results, summarize_on_ci, summary_start_regexp)
    if finalize_async_tasks(outputs, results, skip_cleanup, success_message):
        sys.exit(1)


def print_logs_on_completion(
    include_success_outputs: bool, outputs: list[Output], results: list[ApplyResult]
):
    for i, result in enumerate(results):
        if result.get()[0] != 0:
            message_type = MessageType.ERROR
        else:
            message_type = MessageType.SUCCESS
        if message_type == MessageType.ERROR or include_success_outputs:
            from airflow_breeze.utils.ci_group import ci_group

            with ci_group(f"{outputs[i].escaped_title}", message_type):
                os.write(1, Path(outputs[i].file_name).read_bytes())
        else:
            get_console().print(f"[success]{outputs[i].escaped_title} OK[/]")


def wait_for_all_tasks_completed(poll_time_seconds: float, results: list[ApplyResult]) -> list[ApplyResult]:
    completed_number = 0
    total_number_of_results = len(results)
    completed_list = get_completed_result_list(results)
    while not len(completed_list) == total_number_of_results:
        current_completed_number = len(completed_list)
        if current_completed_number != completed_number:
            completed_number = current_completed_number
            get_console().print(
                f"\n[info]Completed {completed_number} out of {total_number_of_results} "
                f"({completed_number / total_number_of_results:.0%}).[/]\n"
            )
            print_async_result_status(completed_list)
        time.sleep(poll_time_seconds)
        completed_list = get_completed_result_list(results)
    completed_number = len(completed_list)
    get_console().print(
        f"\n[info]Completed {completed_number} out of {total_number_of_results} "
        f"({completed_number / total_number_of_results:.0%}).[/]\n"
    )
    return completed_list


def finalize_async_tasks(
    outputs: list[Output], results: list[ApplyResult], skip_cleanup: bool, success_message: str
) -> bool:
    """
    Finalize async tasks by checking results and cleaning up temporary files.

    :param outputs: List of Output objects containing file names and titles.
    :param results: List of ApplyResult objects containing the results of the tasks.
    :param skip_cleanup: Whether to skip cleanup of temporary files.
    :param success_message: Message to print if all tasks were successful.
    :return: True if there were errors, False otherwise.
    """
    errors = False
    for result in results:
        if result.get()[0] != 0:
            errors = True
    if errors:
        get_console().print("\n[error]There were errors when running some tasks. Quitting.[/]\n")
    else:
        get_console().print(f"\n[success]{success_message}[/]\n")
    if not skip_cleanup:
        for output in outputs:
            Path(output.file_name).unlink(missing_ok=True)
    from airflow_breeze.utils.docker_command_utils import fix_ownership_using_docker

    fix_ownership_using_docker()
    return errors


def summarize_results_outside_of_folded_logs(
    outputs: list[Output],
    results: list[ApplyResult],
    summarize_on_ci: SummarizeAfter,
    summary_start_regexp: str | None = None,
):
    """
    Print summary of the results outside the folded logs in CI.

    :param outputs: List of Output objects containing file names and titles.
    :param results: List of ApplyResult objects containing the results of the tasks.
    :param summarize_on_ci: Determines when to summarize the parallel jobs when they are completed in
        CI, outside the folded CI output.
    :param summary_start_regexp: The regexp that determines line after which
        outputs should be printed as summary, so that you do not have to look at the folded details of
        the run in CI.
    """
    if summarize_on_ci == SummarizeAfter.NO_SUMMARY:
        return
    regex = re.compile(summary_start_regexp) if summary_start_regexp is not None else None
    for i, result in enumerate(results):
        failure = result.get()[0] != 0
        if summarize_on_ci in [
            SummarizeAfter.BOTH,
            SummarizeAfter.FAILURE if failure else SummarizeAfter.SUCCESS,
        ]:
            print_lines = False
            for line in Path(outputs[i].file_name).read_bytes().decode(errors="ignore").splitlines():
                if not print_lines and (regex is None or regex.match(remove_ansi_colours(line))):
                    print_lines = True
                    get_console().print(f"\n[info]Summary: {outputs[i].escaped_title:<30}:\n")
                if print_lines:
                    print(line)


def print_outputs_on_timeout(
    outputs: list[Output], results: list[ApplyResult], include_success_outputs: bool
):
    """
    Print outputs of the tasks that were terminated on timeout.
    This function is called when some tasks were terminated on timeout.
    It prints the outputs of the tasks that were terminated on timeout,
    and the outputs of the tasks that were successful if `include_success_outputs` is True.
    :param outputs: list of Output objects containing file names and titles
    :param results: list of ApplyResult objects containing the results of the tasks
    :param include_success_outputs: whether to include outputs of successful tasks
    """
    get_console().print(
        "\n[warning]Some tasks were terminated on timeout. "
        "Please check the logs of the tasks (below) for more details.[/]\n"
    )
    for i, result in enumerate(results):
        try:
            exit_code = result.get(timeout=0)[0]
        except Exception:
            exit_code = -1
        if exit_code != 0:
            message_type = MessageType.ERROR
        else:
            message_type = MessageType.SUCCESS
        output = outputs[i]
        if message_type == MessageType.ERROR or include_success_outputs:
            from airflow_breeze.utils.ci_group import ci_group

            with ci_group(f"{output.escaped_title}", message_type):
                os.write(1, Path(output.file_name).read_bytes())
        else:
            get_console().print(f"[success]{outputs[i].escaped_title} OK[/]")
    get_console().print(
        "\n[warning]Some tasks were terminated on timeout. "
        "Please check the logs of the tasks (above) for more details.[/]\n"
    )
    from airflow_breeze.utils.docker_command_utils import fix_ownership_using_docker

    fix_ownership_using_docker()


@contextmanager
def run_with_pool(
    parallelism: int,
    all_params: list[str],
    initial_time_in_seconds: int = 2,
    time_in_seconds: int = int(os.environ.get("AIRFLOW_MONITOR_DELAY_TIME_IN_SECONDS", "20")),
    debug_resources: bool = False,
    progress_matcher: AbstractProgressInfoMatcher | None = None,
) -> Generator[tuple[Pool, list[Output]], None, None]:
    get_console().print(f"Running with parallelism: {parallelism}")
    pool = create_pool(parallelism)
    outputs = get_output_files(all_params)
    m = ParallelMonitor(
        outputs=outputs,
        initial_time_in_seconds=initial_time_in_seconds,
        time_in_seconds=time_in_seconds,
        debug_resources=debug_resources,
        progress_matcher=progress_matcher,
    )
    m.start()
    yield pool, outputs
    pool.close()
    pool.join()
