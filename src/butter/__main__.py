import click

from butter.churn import calculate_churn
from butter.git import extract_git_commits, build_file_creation_index


@click.group()
def cli(): ...


@cli.command("build-file-creation-index")
@click.option(
    "-r",
    "--repo",
    "repository_path",
    type=click.Path(exists=True, dir_okay=True, file_okay=False),
    required=True,
)
@click.option(
    "-o",
    "--output-path",
    type=click.Path(exists=False, dir_okay=False, writable=True, file_okay=True),
    required=True,
)
def run_build_file_creation_index(repository_path: str, output_path: str):
    index = build_file_creation_index(repository_path, show_progress=True)
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
@click.option(
    "-o",
    "--output-path",
    type=click.Path(exists=False, dir_okay=False, writable=True, file_okay=True),
    required=True,
)
@click.option("-d", "--days-ago", type=int, default=30)
@click.option("--with-merges", "with_merges", flag_value=True)
def check(
    repository_path: str,
    branch: str,
    cache_path: str,
    output_path: str,
    days_ago: int,
    with_merges: bool = False,
):
    repo_analysis = extract_git_commits(
        repo_path=repository_path,
        branch=branch,
        file_creation_index_path=cache_path,
        days_ago=days_ago,
        with_merges=with_merges,
    )
    churn_analysis = calculate_churn(repo_analysis, days_ago=days_ago)
    churn_analysis.write_parquet(output_path)

    print(churn_analysis.sort(by="total_churn", descending=True).head(20))


if __name__ == "__main__":
    cli()
