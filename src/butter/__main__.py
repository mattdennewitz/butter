import click

from butter.churn import calculate_churn, calculate_churn_by_codeowner
from butter.git import extract_git_commits, build_file_addition_index


@click.group()
def cli(): ...


@cli.command("build-file-addition-index")
@click.option(
    "-r",
    "--repo",
    "repository_path",
    type=click.Path(exists=True, dir_okay=True, file_okay=False),
    required=True,
)
@click.option("-b", "--branch", type=str, default="main", required=True)
@click.option(
    "-o",
    "--output-path",
    type=click.Path(exists=False, dir_okay=False, writable=True, file_okay=True),
    required=True,
)
def run_file_addition_indexer(repository_path: str, branch: str, output_path: str):
    index = build_file_addition_index(
        repository_path, branch=branch, show_progress=True
    )
    index.write_parquet(output_path)


@cli.command()
@click.option(
    "-r",
    "--repository-path",
    type=click.Path(exists=True, dir_okay=True, file_okay=False),
    required=True,
)
@click.option("-b", "--branch", type=str, default="main", required=True)
@click.option(
    "-c",
    "--cache-path",
    type=click.Path(exists=True, dir_okay=False, file_okay=True),
    required=True,
)
@click.option("-d", "--days-ago", type=int, default=30)
@click.option("--with-merge-commits", "with_merge_commits", flag_value=True)
def check(
    repository_path: str,
    branch: str,
    cache_path: str,
    days_ago: int,
    with_merge_commits: bool = False,
):
    repo_analysis_df = extract_git_commits(
        repo_path=repository_path,
        branch=branch,
        file_creation_index_path=cache_path,
        days_ago=days_ago,
        with_merge_commits=with_merge_commits,
    )

    churn_analysis = calculate_churn(repo_analysis_df, days_ago=days_ago)
    churn_by_codeowners_analysis = calculate_churn_by_codeowner(repo_analysis_df)

    churn_analysis.write_csv("churn-analysis.csv")
    churn_by_codeowners_analysis.write_csv("churn-by-codeowners-analysis.csv")


if __name__ == "__main__":
    cli()
