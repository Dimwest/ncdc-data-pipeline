from src.extract import download_zip_files
from src.transform import parse_txt_files
from src.load import get_params_from_env, get_cursor, \
    insert_records, generate_copy_queries

if __name__ == '__main__':

    urls = [
        "https://www1.ncdc.noaa.gov/pub/data/igra/data/data-por/USM00070261-data.txt.zip",
        "https://www1.ncdc.noaa.gov/pub/data/igra/data/data-por/USM00070219-data.txt.zip",
        "https://www1.ncdc.noaa.gov/pub/data/igra/data/data-por/USM00070361-data.txt.zip",
        "https://www1.ncdc.noaa.gov/pub/data/igra/data/data-por/USM00070308-data.txt.zip",
        "https://www1.ncdc.noaa.gov/pub/data/igra/data/data-por/USM00070398-data.txt.zip"
    ]

    params = get_params_from_env()

    filenames = download_zip_files(urls)

    insert_records(
        params=params,
        records=parse_txt_files(filenames)
    )

    generate_copy_queries(
        params=params,
        partition_col="gph",
        partitions_size=1000,
        prefix="ncdc_data",
        delimiter=',',
        header=True
    )
