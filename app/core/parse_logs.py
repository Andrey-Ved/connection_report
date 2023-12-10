import pandas as pd
import pandera as pa

from datetime import datetime
from os import listdir, sep
from pandera.typing import Series, DataFrame
from typing import TypedDict

from app.core.settings import FilePath


class Event(TypedDict):
    create_at: datetime
    canal: str
    user: str
    event: str


class EventsFromLogs(pa.DataFrameModel):
    create_at: Series[datetime]
    canal: Series[str]
    user: Series[str]
    event: Series[str]


EventsFromLogsDF = DataFrame[EventsFromLogs]


def parse_row(row: str) -> Event:
    event_row = {
        'create_at': datetime.strptime(
            row[0:19],
            '%Y.%m.%d-%H:%M:%S',
        )
    }

    row_parts = row[21:].split(">")
    event_row['canal'] = row_parts[0]

    row_parts = row_parts[1][24:].split(" ")
    event_row['user'] = row_parts[0].replace("vpn_", "")

    event_row['event'] = row_parts[2][:-1]

    return Event(**event_row)


def parse_row_list(
        lines: list[str],
) -> list[Event]:
    event_list = []

    for line in lines:
        event = parse_row(line)

        if event['event'] in ('in', 'out'):
            event_list.append(event)

    return event_list


def logs_to_df(
    log_data: list[Event],
) -> EventsFromLogsDF:
    return pd.DataFrame(log_data).convert_dtypes()


def parse_logs_files(
        logs_path: FilePath,
) -> EventsFromLogsDF:
    log_data = []

    for file in listdir(logs_path):
        if file[-4:] == '.log':
            with open(logs_path + sep + file, 'r') as f:
                lines = f.readlines()

            log_data += parse_row_list(lines)

    return logs_to_df(log_data)
