import psycopg2
from psycopg2.extensions import connection, cursor
from os import environ
from typing import Dict, Iterator
from src.log import logger, with_logging
from contextlib import contextmanager
from io import StringIO


def get_params_from_env() -> Dict[str, str]:

    """
    Gets connection parameters from environment variables, fails and logs and error message
    if one of the environment variables is missing.

    :return: a dictionary containing the connection information
    """

    try:

        return {
            "user": environ["POSTGRES_USER"],
            "password": environ["POSTGRES_PASSWORD"],
            "host": environ["POSTGRES_HOST"],
            "port": environ["POSTGRES_PORT"],
            "database": environ["POSTGRES_DB"],
        }

    except KeyError:
        logger.error(
            f"Missing environment variable, make sure that "
            f"the following environment variables are defined: "
            f"POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, "
            f"POSTGRES_PORT, POSTGRES_DB"
        )


@contextmanager
def get_connection(params: Dict[str, str]) -> connection:

    """
    Get a connection using a context manager.

    :param params: database connection parameters dictionary

    :return: psycopg2 connection object
    """

    try:
        conn = psycopg2.connect(**params)
        yield conn
    except psycopg2.Error as e:
        logger.error(f"Psycopg2 driver error: {e}")
        raise e
    except Exception as e:
        logger.error(f"{str(type(e))} during database operation: {e}")
        raise e
    finally:
        # Close database connection.
        logger.debug("Closing database connection")
        conn.close()


@contextmanager
def get_cursor(params: Dict[str, str], commit: bool = True) -> cursor:

    """
    Get a connection cursor using a context manager.

    :param params: database connection parameters dictionary
    :param commit: boolean determining whether changes should be committed

    :return: psycopg2 cursor object
    """

    with get_connection(params) as conn:
        # Acquire cursor from connection
        logger.debug("Obtaining database cursor.")
        cur = conn.cursor()
        try:
            yield cur
            if commit:
                conn.commit()
        finally:
            # Close cursor
            logger.debug("Closing database cursor.")
            cur.close()


@with_logging
def insert_records(
    params: Dict[str, str], records: Iterator[str], chunksize: int = 100000
) -> None:

    """
    Inserts parsed records into database by chunks of size ‘chunksize‘.

    :param params: database connection parameters dictionary
    :param records: parsed records generator
    :param chunksize: desired size of database inserts' chunks

    :return:
    """

    # Define iterator variable and StringIO object
    i = 0
    f = StringIO()

    # For each record in generator ...
    for r in records:

        # ... If the record is the last one in chunk ...
        if i % chunksize == 0:

            # Insert StringIO object data into Postgres
            logger.info(f"Loading chunk of {chunksize} records to Postgres ...")
            f.seek(0)
            with get_cursor(params=params) as cur:
                cur.copy_from(
                    f, f"{environ['POSTGRES_SCHEMA']}.{environ['POSTGRES_TABLE']}"
                )
            logger.info(
                f"Successfully wrote chunk of {chunksize} records to Postgres !"
            )

            # Recreate a new empty StringIO object
            f.close()
            f = StringIO()

        # In all cases, write the record to the current StringIO object and increment the
        # iterator variable
        f.write(r)
        i += 1

    # Write last data chunk to Postgres
    with get_cursor(params=params) as cur:
        cur.copy_from(f, f"{environ['POSTGRES_SCHEMA']}.{environ['POSTGRES_TABLE']}")
        logger.info(f"Successfully wrote last data chunk !")


@with_logging
def generate_csv_files(
    params: Dict[str, str],
    partition_col: str,
    partitions_size: int,
    prefix: str,
    delimiter: str = ",",
    header: bool = True,
) -> None:

    """
    Generates and executes COPY TO queries creating partitioned CSV files.

    :param params: database connection parameters dictionary
    :param partition_col: name of the integer column to partition data by
    :param partitions_size: size of the partitions in the partition_col values
    :param prefix: prefix given to the .csv files created
    :param delimiter: delimiter used when generating .csv files
    :param header: whether the header should be included in .csv files or not
    """

    # Get the max value from the partition column specified
    logger.info(f"Fetching max value for partition column {partition_col} ...")
    with get_cursor(params) as cur:
        cur.execute(
            f"SELECT max({partition_col}) as max_alt "
            f"FROM {environ['POSTGRES_SCHEMA']}.{environ['POSTGRES_TABLE']}"
        )
        partition_max = cur.fetchone()[0]
    logger.info(
        f"Fetched max value for partition column {partition_col}: {partition_max}"
    )

    # Generate the list of values used to create data chunks
    chunks = [x for x in range(0, partition_max + partitions_size, partitions_size)]

    # For each value in chunks list, dynamically generate a COPY query and execute it
    for i, val in enumerate(chunks):
        if i < len(chunks) - 1:

            outfile = f"/tmp/{prefix}_{val}_{chunks[i+1]}.csv"

            q = (
                f"COPY (SELECT * FROM {environ['POSTGRES_SCHEMA']}.{environ['POSTGRES_TABLE']} WHERE "
                f"{partition_col} >= {val} AND {partition_col} < {chunks[i+1]}) "
                f"TO '{outfile}' CSV DELIMITER '{delimiter}' {'HEADER' if header else ''};"
            )

            logger.info(f"Running COPY query: {q}")
            with open(outfile, "w+") as f:
                with get_cursor(params) as cur:
                    cur.copy_expert(q, f)
