from src.extract import download_zip_files
from src.transform import parse_txt_files
from src.load import get_params_from_env, get_cursor, \
    insert_records, generate_copy_queries
from ruamel.yaml import YAML
from fire import Fire


class NcdcPipeline:

    def run_pipeline(self, config_path: str) -> None:

        yaml = YAML()

        with open(config_path, 'r') as f:
            conf = yaml.load(f)

        params = get_params_from_env()

        filenames = download_zip_files(conf["urls"])

        insert_records(
            params=params,
            records=parse_txt_files(filenames)
        )

        generate_copy_queries(
            params=params,
            partition_col=conf["partition_col"],
            partitions_size=conf["partitions_size"],
            prefix=conf["prefix"],
            delimiter=conf["delimiter"],
            header=True
        )


if __name__ == '__main__':

    Fire(NcdcPipeline)
