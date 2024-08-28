import polars as pl


def calculate_churn_by_codeowner(df: pl.DataFrame):
    """
    Calculate churn by codeowner.

    Args:
        df: A DataFrame containing churn data.

    Returns:
        A DataFrame containing the churn summary.
    """

    churn_summary = (
        df.explode("codeowners")
        .group_by("codeowners")
        .agg(
            [
                pl.sum("lines_added").alias("lines_added"),
                pl.sum("lines_deleted").alias("lines_deleted"),
                pl.sum("lines_modified").alias("lines_modified"),
                pl.count("commit_hash").alias("churn_count"),
            ]
        )
    )

    churn_summary = churn_summary.with_columns(
        (
            pl.col("lines_added") + pl.col("lines_deleted") + pl.col("lines_modified")
        ).alias("total_churn")
    )

    churn_summary = churn_summary.filter(pl.col("churn_count").gt(1)).sort(
        "total_churn", descending=False
    )

    return churn_summary


def calculate_churn(df: pl.DataFrame, days_ago: int = 30) -> pl.DataFrame:
    """
    Calculate churn for a DataFrame containing churn data.

    Args:
        df: A DataFrame containing churn data.
        days_ago: How many days into the past to include in analysis.

    Returns:
        A DataFrame containing the churn summary.
    """

    df = df.with_columns(
        (df["commit_date"] - df["file_creation_date"]).alias("file_age")
    )

    # refine only to files added to primary branch within the specified time frame
    df = df.with_columns(
        (df["file_age"].dt.total_days() <= days_ago).alias("is_new_file")
    )
    df = df.filter(df["is_new_file"])

    df = df.with_columns(
        (
            df["lines_added"]
            + df["lines_deleted"]
            + df["lines_modified"]
        ).alias("total_churn")
    )

    churn_summary = df.group_by("filename").agg(
        [
            pl.sum("lines_added").alias("lines_added"),
            pl.sum("lines_deleted").alias("lines_deleted"),
            pl.sum("lines_modified").alias("lines_modified"),
            pl.sum("total_churn").alias("total_churn"),
            pl.count("commit_hash").alias("churn_count"),
        ]
    )

    churn_summary = churn_summary.filter(pl.col("churn_count").gt(1)).sort(
        "total_churn", descending=False
    )

    return churn_summary
