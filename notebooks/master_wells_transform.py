"""Unified master wells transformation utilities."""
from snowflake.snowpark import Session
from snowflake.snowpark import functions as F


def generate_api_columns(df):
    """Ensure API14, API12 and API10 columns exist using Snowpark operations."""
    if "API14" not in df.columns:
        if "API" in df.columns:
            df = df.with_column("API14", F.lpad(F.col("API"), 14, F.lit("0")))
        elif "APINumber" in df.columns:
            df = df.with_column("API14", F.lpad(F.col("APINumber"), 14, F.lit("0")))
        else:
            raise KeyError("No column available to build API14")
    df = df.with_column("API12", F.substring(F.col("API14"), 1, 12))
    df = df.with_column("API10", F.substring(F.col("API14"), 1, 10))
    return df


def join_env_columns(df, env_df):
    """Join environmental well columns using ``API14`` adding ``_env`` suffix."""
    env_cols = [
        F.col(c).alias(c if c == "API14" else f"{c}_env")
        for c in env_df.columns
    ]
    env_renamed = env_df.select(env_cols)
    return df.join(env_renamed, df["API14"] == env_renamed["API14"], how="left")


def join_rbc_columns(df, rbc_df):
    """Join RBC override columns using ``API14`` adding ``_rbc`` suffix."""
    rbc_cols = [
        F.col(c).alias(c if c == "API14" else f"{c}_rbc")
        for c in rbc_df.columns
    ]
    rbc_renamed = rbc_df.select(rbc_cols)
    return df.join(rbc_renamed, df["API14"] == rbc_renamed["API14"], how="left")


def build_table_name(table, database=None, schema=None):
    """Construct a fully qualified table name."""
    if database and schema:
        return f"{database}.{schema}.{table}"
    if schema:
        return f"{schema}.{table}"
    return table


def transform_master_wells_table(session: Session, env_table: str, rbc_table: str):
    """Create the master wells table by combining and enhancing well datasets."""
    rb_energy_wells = session.table("TG40.TZ_ADA_MP_RB_ENERGY.RB_ENERGY_WELLS")
    env_df = session.table(env_table)
    rbc_df = session.table(rbc_table)

    combined = rb_energy_wells.union_by_name(env_df)
    combined = generate_api_columns(combined)
    combined = combined.with_column("WELL_NAME_UPPER", F.upper(F.col("WELL_NAME")))
    combined = join_env_columns(combined, env_df)
    combined = join_rbc_columns(combined, rbc_df)
    return combined


def main(
    session: Session,
    *,
    database: str | None = None,
    schema: str | None = None,
    output_table: str = "MASTER_WELLS_TABLE_TRANSFORMED",
    env_table: str = "RAW_ENV_PRISM_WELLS",
    rbc_table: str = "RBC_WELL_OVERRIDES",
):
    """Execute the transformation and persist the result."""
    transformed = transform_master_wells_table(session, env_table, rbc_table)
    full_table_name = build_table_name(output_table, database, schema)
    transformed.write.save_as_table(full_table_name, mode="overwrite")
    return transformed


if __name__ == "__main__":
    # Example usage with connection parameters stored elsewhere.
    sess = Session.builder.get_or_create()
    main(sess)
