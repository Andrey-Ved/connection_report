import matplotlib.pyplot as plt
import pandas as pd

from functools import reduce
from json import dump
from operator import add

from app.core.report_data import Users, LogsDF, ReportDF
from app.core.settings import FilePath


def print_users_by_channels(
        users_by_canals: Users,
) -> None:
    users = sorted(
        reduce(
            add,
            (i for k, i in users_by_canals.items())
        )
    )

    print(
        '\n',
        'in total'.rjust(15),
        '-',
        len(users), users,
        '\n',
    )
    print(
        'by channels'.rjust(15),
        ':',
    )
    for canal in users_by_canals:
        print(
            str(canal).rjust(15),
            "-",
            len(users_by_canals[canal]), users_by_canals[canal],
        )
    print()


def draw_graphs_on_total_connections(
        mikrotiks_logs: LogsDF,  # noqa
        users_by_canals: Users,
        max_con_file: FilePath,
        mean_con_file: FilePath,
) -> None:
    canals = sorted([k for k in users_by_canals])
    users_log_df = mikrotiks_logs.T

    # counting user events per hour for each channel
    canals_events_hours_df = []

    for canal in canals:
        canal_events_hours_df = pd.DataFrame(
            users_log_df[users_by_canals[canal]].notna().sum(axis=1),
            columns=[canal],
        )
        if len(canals_events_hours_df) == 0:
            canals_events_hours_df = canal_events_hours_df
        else:
            canals_events_hours_df = canals_events_hours_df \
                .join(canal_events_hours_df)

    # grouping by days and count the hours
    # with registered events for each channel
    canals_events_day_df = canals_events_hours_df.copy()
    canals_events_day_df.index = canals_events_day_df \
        .index.date
    canals_events_day_df.index.name = 'date'
    canals_events_day_max_df = canals_events_day_df \
        .groupby('date').agg('max')
    canals_events_day_mean_df = canals_events_day_df \
        .groupby('date').agg('mean')

    canals_events_day_max_df.plot.bar(
        rot=90,
        title="maximum of total connections"
    )
    plt.savefig(
        max_con_file,
        dpi=300
    )
    # plt.show()

    canals_events_day_mean_df.plot.bar(
        rot=90,
        title="mean_of_total_connections"
    )
    plt.savefig(
        mean_con_file,
        dpi=300
    )
    # plt.show()


def report_to_csv_file(
        df: ReportDF,
        file_name: FilePath,
) -> None:
    df.to_csv(
        path_or_buf=file_name,
        sep=';',
        index=True,
    )


def users_to_json_file(
        users: Users,
        file_name: FilePath,
) -> None:
    with open(file_name, 'w', encoding='utf-8') as file:
        dump(
            obj=users,
            fp=file,
            indent=4,
            sort_keys=True,
            ensure_ascii=False,
        )
