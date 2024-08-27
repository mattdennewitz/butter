import polars as pl


def calculate_churn(churn_df: pl.DataFrame, days_ago: int = 30) -> pl.DataFrame:
    """
    Calculate churn for a DataFrame containing churn data.

    Args:
        churn_df: A DataFrame containing churn data.
        days_ago: How many days into the past to include in analysis.

    Returns:
        A DataFrame containing the churn summary.
    """

    churn_df = churn_df.with_columns(
        (churn_df["commit_date"] - churn_df["file_creation_date"]).alias("file_age")
    )

    # refine only to files added to primary branch within the specified time frame
    churn_df = churn_df.with_columns(
        (churn_df["file_age"].dt.total_days() <= days_ago).alias("is_new_file")
    )
    churn_df = churn_df.filter(churn_df["is_new_file"])

    churn_df = churn_df.with_columns(
        (
            churn_df["lines_added"]
            + churn_df["lines_deleted"]
            + churn_df["lines_modified"]
        ).alias("total_churn")
    )

    churn_summary = churn_df.group_by("file").agg(
        [
            pl.sum("lines_added").alias("lines_added"),
            pl.sum("lines_deleted").alias("lines_deleted"),
            pl.sum("lines_modified").alias("lines_modified"),
            pl.sum("total_churn").alias("total_churn"),
            pl.count("commit_hash").alias("churn_count"),
        ]
    )

    return churn_summary
