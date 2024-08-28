import pathlib
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Iterable, Iterator

from git import Repo
import polars as pl
from tqdm import tqdm

from butter.git.codeowners import CodeOwnersParser


def git_date_to_datetime(value: str) -> datetime:
    return datetime.strptime(value, "%a, %d %b %Y %H:%M:%S %z")


def parse_added_date_from_log(
    repo: Repo, branch: str, filename: str
) -> datetime | None:
    """
    Parse the date when a file was added to the repository.

    Args:
        repo: The repository object.
        branch: The branch to check.
        filename: The name of the file to check.

    Returns:
        The date when the file was added to the repository's main branch.
    """

    log_output = repo.git.execute(
        [
            "git",
            "log",
            branch,
            "--diff-filter=A",
            "--format=%aD",
            "--date=short",
            "--name-only",
            "--follow",
            "--",
            filename,
        ]
    ).strip()

    if log_output:
        date_committed = log_output.splitlines()[0]
        return git_date_to_datetime(date_committed)

    # file could not be found. check if it was part of a merge commit.
    log_output = repo.git.execute(
        [
            "git",
            "log",
            branch,
            "--format=%aD",
            "--date=short",
            "--merges",
            "--",
            filename,
        ]
    ).strip()

    if log_output:
        date_committed = log_output.splitlines()[-1]
        return git_date_to_datetime(date_committed)


def build_file_addition_index(
    repo_path: str, branch: str, show_progress: bool = False
) -> pl.DataFrame:
    """
    Build a DataFrame containing the date when each file was added to the main branch.

    Args:
        repo_path: The path to the repository.
        branch: The branch to check.
        show_progress: Whether to show a progress bar.

    Returns:
        A DataFrame containing the date when each file was added to the repository.
    """

    repo = Repo(repo_path)

    ls_output = repo.git.execute(["git", "ls-tree", "-r", branch, "--name-only"])
    filenames = [filename.strip() for filename in ls_output.split("\n")]

    def git_history_iterator(repo: Repo, filenames: Iterable[str]) -> Iterator[dict]:
        with ThreadPoolExecutor() as executor:
            future_to_filename_tasks = {
                executor.submit(
                    parse_added_date_from_log, repo, branch, filename
                ): filename
                for filename in filenames
            }

            for future in as_completed(future_to_filename_tasks):
                filename = future_to_filename_tasks[future]

                try:
                    added_date = future.result()
                except Exception as e:
                    print(f"An error occurred while processing {filename}: {e}")

                yield {"filename": filename, "added_date": added_date}

    iterator = git_history_iterator(repo, filenames)
    if show_progress:
        iterator = iter(tqdm(iterator, total=len(filenames)))

    return pl.DataFrame(iterator)


def find_codeowners(repo: Repo) -> pathlib.Path | None:
    """
    Find CODEOWNERS files in the repository.

    Args:
        repo: The repository object.

    Returns:
        A list of CODEOWNERS files in the repository.
    """

    codeowners_files = repo.git.execute(
        ["git", "ls-files", "--", "CODEOWNERS", "CODEOWNERS.md"]
    ).splitlines()

    if codeowners_files:
        return pathlib.Path(repo.working_tree_dir) / codeowners_files[0]

    return codeowners_files


def extract_git_commits(
    repo_path: str,
    branch: str = "main",
    start_date: datetime = None,
    end_date: datetime = None,
    file_creation_index_path: str = None,
    days_ago=30,
    with_merge_commits: bool = False,
) -> pl.DataFrame:
    """
    Extract file-level details from Git commits in a given timespan.

    Args:
        repo_path: The path to the repository.
        branch: The branch to check.
        start_date: The start date for the analysis.
        end_date: The end date for the analysis.
        file_creation_index_path: The path to the file creation index.
        days_ago: How many days into the past to include in analysis.
        with_merge_commits: Whether to include merge commits in the analysis. This is not recommended for churn analysis.

    Returns:
        A DataFrame containing the churn data.
    """

    repo = Repo(repo_path)

    # read file creation dates from cache.
    # no set_index method in polars makes this silly.
    file_creation_dates = (
        pl.read_parquet(file_creation_index_path)
        .select(["filename", "added_date"])
        .to_dict(as_series=False)
    )
    file_creation_dates = dict(
        zip(file_creation_dates["filename"], file_creation_dates["added_date"])
    )

    if start_date is None:
        start_date = datetime.now() - timedelta(days=days_ago)
    if end_date is None:
        end_date = datetime.now()

    # blend in codeowners data
    if codeowners_file := find_codeowners(repo):
        codeowners_data = codeowners_file.open("r").read()
    else:
        codeowners_data = ""  # hacky, i know
    codeowners_parser = CodeOwnersParser(codeowners_data)

    commits = repo.iter_commits(
        rev=branch,
        since=start_date.isoformat(),
        until=end_date.isoformat(),
        no_merges=not with_merge_commits,
    )

    churn_data = []

    for commit in commits:
        for filename, stats in commit.stats.files.items():
            if filename not in file_creation_dates:
                continue

            churn_data.append(
                {
                    "commit_hash": commit.hexsha,
                    "filename": filename,
                    "lines_added": stats["insertions"],
                    "lines_deleted": stats["deletions"],
                    "lines_modified": stats["lines"],
                    "commit_date": commit.committed_datetime,
                    "file_creation_date": file_creation_dates[filename],
                    "codeowners": codeowners_parser.get_owners(filename),
                }
            )

    return pl.DataFrame(churn_data)
