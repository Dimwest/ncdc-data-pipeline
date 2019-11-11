from src.extract import download_zip_files
from src.transform import parse_txt_files
from src.load import get_params_from_env, get_cursor, insert_records, generate_csv_files
from ruamel.yaml import YAML
from fire import Fire


class NcdcPipeline:
    @staticmethod
    def run_pipeline(config_path: str) -> None:

        """
        Runs the entire NCDC data pipeline

        :param config_path: path to the .yml configuration file
        """

        # Load config en database parameters
        yaml = YAML()
        with open(config_path, "r") as f:
            conf = yaml.load(f)
        params = get_params_from_env()

        # # Download, extract and delete .zip files
        # filenames = download_zip_files(conf["urls"])
        #
        # # Transform records and load them into Postgres using COPY FROM
        # insert_records(params=params, records=parse_txt_files(filenames),
        #                chunksize=conf["insert_chunksize"])

        # Prepare and execute COPY TO query exporting data to partitioned .csv files
        generate_csv_files(
            params=params,
            partition_col=conf["partition_col"],
            partitions_size=conf["partitions_size"],
            prefix=conf["prefix"],
            delimiter=conf["delimiter"],
            header=conf["header"]
        )


if __name__ == "__main__":

    Fire(NcdcPipeline)
