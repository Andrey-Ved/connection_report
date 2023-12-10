import numpy as np
import pandas as pd

from datetime import timedelta, datetime
from pandas.tseries.offsets import DateOffset

from app.core.parse_logs import EventsFromLogsDF


UserName = str
Users = dict[str, list[UserName]]


class LogsDF(pd.DataFrame):
    """columns: list[UserName], index: datetime(freq='h'), value: str|np.nan"""
    pass


class ReportDF(pd.DataFrame):
    """columns: list[UserName], index: datetime(freq='D'), values=int"""
    pass


def gap_filling(
        series: pd.Series,
) -> pd.Series:
    ser = series.fillna('')

    last_event = 'out'

    for i in range(0, len(ser)):
        if ser.iloc[i] in ('in', 'out'):
            last_event = ser.iloc[i]
        else:
            ser.iloc[i] = 'in' if last_event == 'in' \
                else np.nan

    return ser


def create_report_data(
        data_from_logs: EventsFromLogsDF,
        first_date: datetime,
        final_date: datetime,
) -> tuple[LogsDF, Users]:
    logs_data = data_from_logs.copy(deep=True)

    # choosing time period
    logs_data = logs_data[(logs_data['create_at'] >= first_date)
                          &
                          (logs_data['create_at'] < (final_date + DateOffset())
                           )]  # noqa

    # creating list of channels
    canals = sorted(
        map(
            str,
            logs_data['canal'].unique()
        )
    )

    # creating a dictionary containing a list of users for each channel
    users_by_canals = {}

    for canal in canals:
        users_by_canals[canal] = sorted(
            map(
                str,
                logs_data[logs_data['canal'] == canal]['user'].unique()
            )
        )

    # fixing users names duplicate
    processed_users = set()

    for canal in users_by_canals:
        for i in range(len(users_by_canals[canal])):
            user = users_by_canals[canal][i]  # noqa

            user_name = user
            count = 1

            while user_name in processed_users:
                count += 1
                user_name = user + str(count)

            if user_name != user:
                mask = (logs_data['canal'] == canal) & (logs_data['user'] == user)
                logs_data.loc[mask, 'user'] = user_name
                users_by_canals[canal][i] = user_name

            processed_users.add(user_name)

    users = sorted(
        list(processed_users)
    )

    logs_data.drop('canal', axis=1, inplace=True)

    logs_data['hours'] = logs_data['create_at'].dt.floor(freq='H')

    # creating time series for the supplement
    hours_range = pd.date_range(
        first_date,
        final_date + DateOffset() - timedelta(hours=1),
        freq='h'
    )
    time_series_for_add = pd.DataFrame(
        {'event': pd.Series([''] * len(hours_range)),
         'hours': hours_range}
    )
    time_series_for_add = time_series_for_add.set_index('hours')

    # processing by users
    users_log = pd.DataFrame()

    for user in users:
        user_log = logs_data[logs_data['user'] == user]. \
            drop('user', axis=1)
        user_log.set_index('create_at', inplace=True)
        user_log = user_log.groupby('hours').agg('last')
        user_log = user_log.reset_index()

        user_log['hours'] = user_log['hours'].dt.floor(freq='H')
        user_log.set_index('hours', inplace=True)

        # adding missing hours
        user_log = user_log.add(time_series_for_add)

        # filling in the missing hours
        user_log = user_log.apply(gap_filling, axis=0)

        user_log.rename(columns={'event': user}, inplace=True)

        # assembling the total dataframe
        if len(users_log) == 0:
            users_log = user_log
        else:
            users_log = users_log.join(user_log)

    return LogsDF(users_log.T), users_by_canals


def aggregation_with_hours_counting(
        users_log: LogsDF,
) -> ReportDF:
    users_events_day = users_log.copy(deep=True)

    users_events_day.index.name = 'user'
    users_events_day = users_events_day.T
    users_events_day = users_events_day.reset_index()
    users_events_day['hours'] = users_events_day['hours'] \
        .dt.floor(freq='D')
    users_events_day = users_events_day.groupby('hours') \
        .agg('count')
    users_events_day.index.name = 'date'

    return ReportDF(users_events_day.T)
