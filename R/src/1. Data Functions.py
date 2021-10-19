# Connection
import snowflake.connector

import tempfile
import os
import pandas as pd
import numpy as np


def get_connections_fn(OKTA_USER, OKTA_PASSWORD):
    """
    Returns a connection getting function for
    Snowflake
    """

    def get_connections(schema):

        con = snowflake.connector.connect(
            user=OKTA_USER,
            password=OKTA_PASSWORD,
            account="ly01550",
            role="WNG_BI_ANALYSTS",
            warehouse="GROUP_GENERAL",
            database="WNG_DEV",
            schema="LEGACY_DW",
            region="ap-southeast-2",
            # authenticator="https://nib.okta.com/app/snowflake/exk800eg1nnCkTCLD1t7/sso/saml",
            authenticator="externalbrowser",
            insecure_mode=True,
        )

        tmp = snowflake.connector.connect(
            user=OKTA_USER,
            password=OKTA_PASSWORD,
            account="ly01550",
            role="WNG_BI_ANALYSTS",
            warehouse="GROUP_GENERAL",
            database="WNG_PROD",
            schema="TRANSIENT",
            region="ap-southeast-2",
            # authenticator="https://nib.okta.com/app/snowflake/exk800eg1nnCkTCLD1t7/sso/saml",
            authenticator="externalbrowser",
            insecure_mode=True,
        )

        if schema == "TRANSIENT":
            return tmp
        else:
            return con

    # Test connection
    get_connections("TRANSIENT")

    return get_connections


def chunked_data(con, sql, chunksize):
    """
    Read data in chunks.
    """
    return pd.read_sql(
        sql, con, chunksize=chunksize, coerce_float=True, parse_dates=True
    )


def read_data(con, sql):
    """
    Read data in chunks.
    """
    return pd.read_sql(sql, con, coerce_float=True, parse_dates=True)


def sqltype(col):
    """
    Add name, varchar if string, number otherwise
    """
    if col.dtype == object:
        return col.name + " varchar"
    elif col.dtype == "datetime64[ns]":
        return col.name + " TIMESTAMP_NTZ"
    elif col.dtype == "bool":
        return col.name + " BOOLEAN"
    else:
        return col.name + " number(18, 9)"


def snowflake_sql_types(df):
    """
    Returns sql types for Snowflake for a given Pandas DataFrames
    """

    return [sqltype(df[x]) for x in df.columns]


def snowflake_create_query(df, table_name):
    """
    CREATE or REPLACE TABLE.
    """
    query = (
        f"CREATE or REPLACE TABLE {table_name} ("
        + ", ".join(snowflake_sql_types(df))
        + ")"
    )
    return query


def upload_table(df, tmp, table_name, number_of_chunks=10):
    """
    Uploads a table.
    df: DataFrame
    tmp: Transient connection
    number_of_chunks:
    """
    print(f"Uploading table: {table_name}")
    print(df.dtypes)

    query = snowflake_create_query(df, table_name)
    tmp.cursor().execute(query)
    tmp.cursor().execute(f"CREATE or REPLACE TEMPORARY STAGE {table_name}")

    # Split into 10 chunks
    tempfilelist = []
    for id, df_i in enumerate(np.array_split(df, number_of_chunks)):
        with tempfile.NamedTemporaryFile() as tempf:
            print(tempf.name)
            df_i.to_csv(
                tempf.name + ".csv.gz",
                index=False,
                header=False,
                float_format="%.6g",
                quoting=csv.QUOTE_NONE,
                escapechar="\\",
                compression="gzip",
            )
            tmp.cursor().execute(f"PUT file://{tempf.name}.csv.gz @%{table_name}")
            tempfilelist = tempfilelist + [tempf.name + ".csv.gz"]

    tmp.cursor().execute(f"COPY INTO {table_name} purge = true")

    for tempf_name in tempfilelist:
        os.unlink(tempf_name)
