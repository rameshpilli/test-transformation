"""Snowflake transformation functions for master wells table."""
from snowflake.snowpark import Session
from snowflake.snowpark import functions as F


def load_table(session: Session, table_name: str):
    """Return a DataFrame for the given table name."""
    return session.table(table_name)


def union_tables(df1, df2):
    """Union two DataFrames by column name."""
    return df1.union_by_name(df2)


def join_tables(df_left, df_right, join_exprs, how: str = "left"):
    """Join two DataFrames using the provided join expressions."""
    return df_left.join(df_right, join_exprs, how)


def derive_columns(df):
    """Add standard derived columns used in the master wells table."""
    return (
        df.with_column("API_WELL_NUMBER", F.col("API_WELL_NUMBER").cast("string"))
          .with_column("WELL_NAME_UPPER", F.upper(F.col("WELL_NAME")))
    )


def transform_master_wells_table(session: Session):
    """Create the master wells table by combining and enhancing well datasets."""
    rb_energy_wells = load_table(session, "TG40.TZ_ADA_MP_RB_ENERGY.RB_ENERGY_WELLS")
    raw_env_prism_wells = load_table(session, "RAW_ENV_PRISM_WELLS")
    rbc_well_overrides = load_table(session, "RBC_WELL_OVERRIDES")

    combined = union_tables(rb_energy_wells, raw_env_prism_wells)
    enhanced = derive_columns(combined)
    master_df = join_tables(
        enhanced,
        rbc_well_overrides,
        enhanced["API_WELL_NUMBER"] == rbc_well_overrides["API_WELL_NUMBER"],
        how="left",
    )
    return master_df


def build_table_name(table: str, database: str | None = None, schema: str | None = None) -> str:
    """Construct a fully qualified table name."""
    if database and schema:
        return f"{database}.{schema}.{table}"
    if schema:
        return f"{schema}.{table}"
    return table


def main(
    session: Session,
    database: str | None = None,
    schema: str | None = None,
    table_name: str = "MASTER_WELLS_TABLE_TRANSFORMED",
):
    """Execute the transformation and persist the result."""
    transformed = transform_master_wells_table(session)
    full_table_name = build_table_name(table_name, database, schema)
    transformed.write.save_as_table(full_table_name, mode="overwrite")
    return transformed


if __name__ == "__main__":
    # Example usage with connection parameters stored elsewhere.
    sess = Session.builder.get_or_create()
    main(sess)
