import pandas as pd

from datetime import datetime
from typing import Union

from dotenv import load_dotenv
from os import getenv, makedirs, path as os_path


FilePath = Union[str, "PathLike[str]"]

ROOT_PATH = os_path.dirname(
    os_path.dirname(
        os_path.dirname(
            os_path.abspath(__file__)
        )
    )
)

ENV_FILE = ROOT_PATH + os_path.sep + ".env"

if os_path.exists(ENV_FILE):
    load_dotenv(dotenv_path=ENV_FILE)
else:
    print(f'{ENV_FILE} - not exist')
    raise Exception

FIRST_DATE = datetime.strptime(
    getenv('FIRST_DATE', '2021-04-12'),  # (2021, 9, 25)
    '%Y-%m-%d',
)
FINAL_DATE = datetime.strptime(
    getenv('FINAL_DATE', '2021-06-21'),  # (2021, 10, 26)
    '%Y-%m-%d',
)

JSON_USERS_FILE = getenv(
    'JSON_USERS_FILE',
    'users-out.json',
)
CSV_REPORT_FILE = getenv(
    'CSV_REPORT_FILE',
    'report-out.csv',
)
XLSX_REPORT_FILE = getenv(
    'XLSX_REPORT_FILE',
    'report-out.xlsx',
)
MAX_CON_FILE = getenv(
    'MAX_CONNECTION_FILE',
    'maximum_of_total_connections.png'
)
MEAN_CON_FILE = getenv(
    'MEAN_CONNECTION_FILE',
    'mean_of_total_connections.png'
)

LOGS_PATH = getenv(
    'LOGS_PATH',
    ROOT_PATH + os_path.sep + 'data' + os_path.sep,
)
OUT_PATH = getenv(
    'OUT_PATH',
    ROOT_PATH + os_path.sep + 'out',
)

OUT_PATH += os_path.sep \
            + f'{FIRST_DATE.strftime("%Y%m%d")}' \
            + f'-' \
            + f'{FINAL_DATE.strftime("%Y%m%d")}' \
            + os_path.sep

if not os_path.isdir(OUT_PATH):
    makedirs(OUT_PATH)

JSON_USERS_PATH = OUT_PATH + JSON_USERS_FILE
CSV_REPORT_PATH = OUT_PATH + CSV_REPORT_FILE
XLSX_REPORT_PATH = OUT_PATH + XLSX_REPORT_FILE
PNG_REPORT_MAX_CON_PATH = OUT_PATH + MAX_CON_FILE
PNG_REPORT_MEAN_CON_PATH = OUT_PATH + MEAN_CON_FILE

pd.set_option('display.max_columns', 10)
pd.set_option('display.width', 380)
pd.set_option('display.max_rows', 30)
