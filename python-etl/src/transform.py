from src.log import logger, with_logging
from os import remove
from typing import Iterator, Dict, List


def parse_header(header: str) -> Dict[str, str]:

    """
    Parses header row into a dictionary for further processing.

    :param header: header row string

    :return: header dictionary
    """

    # ---------------------------------
    # Variable   Columns  Type
    # ---------------------------------
    # HEADREC       1-  1  Character
    # ID            2- 12  Character
    # YEAR         14- 17  Integer
    # MONTH        19- 20  Integer
    # DAY          22- 23  Integer
    # HOUR         25- 26  Integer
    # RELTIME      28- 31  Integer
    # NUMLEV       33- 36  Integer
    # P_SRC        38- 45  Character
    # NP_SRC       47- 54  Character
    # LAT          56- 62  Integer
    # LON          64- 71  Integer
    # ---------------------------------

    return {
        "headrec": header[0:1],
        "id": header[1:12],
        "year": header[13:17],
        "month": header[18:20],
        "day": header[21:23],
        "hour": header[24:26],
        "reltime": header[27:31],
        "numlev": header[32:36],
        "p_src": header[37:45],
        "np_src": header[46:54],
        "lat": header[55:62],
        "lon": header[63:71],
    }


def parse_data_record(data: str) -> Dict[str, str]:

    """
    Parses data record row into a dictionary for further processing.

    :param data: data row string

    :return: data record dictionary
    """

    # -------------------------------
    # Variable        Columns Type
    # -------------------------------
    # LVLTYP1         1-  1   Integer
    # LVLTYP2         2-  2   Integer
    # ETIME           4-  8   Integer
    # PRESS          10- 15   Integer
    # PFLAG          16- 16   Character
    # GPH            17- 21   Integer
    # ZFLAG          22- 22   Character
    # TEMP           23- 27   Integer
    # TFLAG          28- 28   Character
    # RH             29- 33   Integer
    # DPDP           35- 39   Integer
    # WDIR           41- 45   Integer
    # WSPD           47- 51   Integer
    # -------------------------------

    return {
        "lvltyp1": data[0:1],
        "lvltyp2": data[1:2],
        "etime": data[3:8],
        "press": data[9:15],
        "pflag": data[15:16],
        "gph": data[16:21],
        "zflag": data[21:22],
        "temp": data[22:27],
        "tflag": data[27:28],
        "rh": data[28:33],
        "dpdp": data[34:39],
        "wdir": data[40:45],
        "wspd": data[46:51],
    }


def transform_record(record: Dict[str, str]) -> str:

    """
    Transforms a record in several steps:
        1 - drops specified columns
        2 - splits hhmm formatted strings
        3 - splits mmmss formatted strings
        4 - casts specified columns to int type
        5 - replaces empty string values with None

    :param record: dictionary to transform
    :return: transformed dictionary
    """

    # Drop unused columns
    to_drop = ["headrec", "numlev"]
    for c in to_drop:
        record.pop(c)

    # Split hours and minutes from HHMM strings
    split_hh_mm = ["reltime"]
    for c in split_hh_mm:
        record[f"{c}_hh"] = record[c][:2]
        record[f"{c}_mm"] = record[c][2:]
        record.pop(c)

    # Convert string values into integers, handle rare cases of empty string data by replacing
    # with Postgres' COPY command's default null value "\\N"
    to_int = [
        "year",
        "month",
        "day",
        "hour",
        "lat",
        "lon",
        "reltime_hh",
        "reltime_mm",
        "lvltyp1",
        "lvltyp2",
        "etime",
        "temp",
        "rh",
        "dpdp",
        "wdir",
        "wspd",
    ]
    for c in to_int:
        try:
            record[c] = int(record[c])
        except ValueError:
            record[c] = "\\N"

    # Replace blank strings with Postgres' COPY command's default null value "\\N"
    blank_to_null = ["p_src", "np_src", "pflag", "zflag", "tflag"]
    for c in blank_to_null:
        record[c] = record[c].replace(" ", "")
        if not record[c]:
            record[c] = "\\N"

    # Convert dict to Postgres' COPY command string and return it
    return dict_to_str(record)


def dict_to_str(r: Dict) -> str:

    """
    Transforms a dictionary record into a string for StringIO insert

    :param r: record dictionary

    :return: a string compliant with Postgres' COPY command format
    """

    io_str = (
        f"{r['id']}\t{r['year']}\t{r['month']}\t{r['day']}\t{r['hour']}\t"
        f"{r['p_src']}\t{r['np_src']}\t{r['lat']}\t{r['lon']}\t{r['lvltyp1']}\t"
        f"{r['lvltyp2']}\t{r['press']}\t{r['pflag']}\t{r['gph']}\t{r['zflag']}\t{r['temp']}\t"
        f"{r['tflag']}\t{r['rh']}\t{r['dpdp']}\t{r['wdir']}\t{r['wspd']}\t{r['reltime_hh']}\t"
        f"{r['reltime_mm']}\t{r['etime']}\n"
    )

    return io_str


@with_logging
def parse_txt_files(paths: List[str]) -> Iterator[str]:

    """
    Parses a NCDC data file stored locally and yields database records from it.

    :param paths: list of .txt files paths

    :return: database records generator
    """

    for p in paths:
        with open(p, "r") as f:
            for line in f:
                if line.startswith("#"):
                    header = parse_header(line)
                else:
                    data = parse_data_record(line)
                    yield transform_record({**header, **data})
        remove(p)
        logger.info(f"Successfully parsed and deleted text file {p} !")
