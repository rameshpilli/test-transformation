import pandas as pd
from pathlib import Path


def generate_api_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Generate API-related columns.

    This builds an ``API14`` column from any present API column and derives
    ``API12`` and ``API10`` as short forms.
    """
    if 'API14' not in df.columns:
        if 'API' in df.columns:
            df['API14'] = df['API'].astype(str).str.zfill(14)
        elif 'APINumber' in df.columns:
            df['API14'] = df['APINumber'].astype(str).str.zfill(14)
        else:
            raise KeyError('No column available to build API14')
    df['API12'] = df['API14'].str[:12]
    df['API10'] = df['API14'].str[:10]
    return df


def join_env_columns(df: pd.DataFrame, env_df: pd.DataFrame) -> pd.DataFrame:
    """Join environmental well columns using ``API14``."""
    return df.merge(env_df, on='API14', how='left', suffixes=(None, '_env'))


def join_rbc_columns(df: pd.DataFrame, rbc_df: pd.DataFrame) -> pd.DataFrame:
    """Join RBC override columns using ``API14``."""
    return df.merge(rbc_df, on='API14', how='left', suffixes=(None, '_rbc'))


def main() -> None:
    project_dir = Path(__file__).resolve().parents[1]
    data_dir = project_dir / 'data'

    master_df = pd.read_csv(data_dir / 'raw_master_wells_table.csv', dtype=str)
    env_df = pd.read_csv(data_dir / 'raw_env_prism_wells.csv', dtype=str)
    rbc_df = pd.read_csv(data_dir / 'rbc_well_overrides.csv', dtype=str)

    master_df = generate_api_columns(master_df)
    master_df = join_env_columns(master_df, env_df)
    master_df = join_rbc_columns(master_df, rbc_df)

    outputs_dir = project_dir / 'outputs'
    outputs_dir.mkdir(exist_ok=True)
    master_df.to_csv(outputs_dir / 'master_wells_table_transformed.csv', index=False)


if __name__ == '__main__':
    main()
