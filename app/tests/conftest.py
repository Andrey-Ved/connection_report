import pandas as pd
import pytest

from datetime import timedelta, datetime
from random import shuffle, randint
from typing import TypedDict

from app.core.parse_logs import Event
from app.core.report_data import ReportDF
from app.core.settings import FilePath


class State(TypedDict):
    date: datetime
    canal: str
    user: str
    worked_hours: int


def disassemble_event_to_row(
        event: Event
) -> str:
    return f'{event["create_at"].strftime("%Y.%m.%d-%H:%M:%S")} ' \
           f'<{event["canal"]}>: l2tp,ppp,info,account ' \
           f'vpn_{event["user"]} logged {event["event"]}, \n'


def disassemble_events_list_into_row_list(
        events: list[Event]
) -> list[str]:
    row_list = []

    for event in events:
        row_list.append(
            disassemble_event_to_row(
                event
            )
        )

    return row_list


def random_disassemble_state_into_events(
        state: State
) -> list[Event]:
    events_list = []

    worked_hour_list = [True] * state['worked_hours']
    worked_hour_list += [False] * (24 - state['worked_hours'])

    shuffle(worked_hour_list)

    number_events_at_hours = [0] * 24

    for h_id in range(0, 24):
        if worked_hour_list[h_id]:
            number_events_at_hours[h_id] = randint(2, 4)

    con = {0: 'out', 1: 'in'}

    events_at_hours = []

    for h_id in range(0, 24):
        events_at_hours.append(
            [
                con[randint(0, 1)]
                for _ in range(number_events_at_hours[h_id])
            ]
        )

    for h_id in range(0, 23):
        if worked_hour_list[h_id]:
            events_at_hours[h_id][-1] = con[1] \
                if worked_hour_list[h_id + 1] \
                else con[0]

    if worked_hour_list[23]:
        events_at_hours[23][-1] = con[0]

    for h_id in range(0, 24):
        if events_at_hours[h_id]:
            for e_id in range(len(events_at_hours[h_id])):
                date = state['date'] + timedelta(hours=h_id) \
                       + timedelta(minutes=10 * (e_id + 1))

                events_list += [
                    Event(
                        create_at=date,
                        canal=state['canal'],
                        user=state['user'],
                        event=events_at_hours[h_id][e_id]
                    )
                ]

    return events_list


def disassemble_states_list_into_events_list(states_list: list[State]) -> list[Event]:
    events_list = []

    for state in states_list:
        events_list += random_disassemble_state_into_events(state)

    return events_list


def disassemble_report_into_states_list(report: ReportDF) -> list[State]:
    states_list = []
    columns_name = report.columns.tolist()

    for date_id in range(1, len(columns_name)):
        for user_id in range(len(report)):
            states_list.append(
                State(
                    date=columns_name[date_id],
                    canal='192.168.0.1',
                    user=report.iloc[user_id].iloc[0],
                    worked_hours=report.iloc[user_id].iloc[date_id],
                )
            )

    return states_list


class Oracle:
    def __init__(self, test_data_file: FilePath):
        self.report = pd.read_csv(
            filepath_or_buffer=test_data_file,
            sep=';',
        )
        columns_name = self.report.columns.tolist()
        columns_name[0] = 'user'

        for i in range(1, len(columns_name)):
            columns_name[i] = datetime.strptime(
                columns_name[i],
                '%Y-%m-%d',
            )

        self.report.columns = columns_name

        self.first_date = columns_name[1]
        self.final_date = columns_name[-1]

        self.raw_logs = disassemble_events_list_into_row_list(
            disassemble_states_list_into_events_list(
                disassemble_report_into_states_list(
                    self.report
                )
            )
        )

        self.report.set_index('user', inplace=True)
        self.report = self.report.T
        self.report.index.name = 'hours'
        self.report.reset_index(inplace=True)
        self.report['hours'] = self.report['hours']\
            .dt.floor(freq='D')
        self.report.set_index('hours', inplace=True)
        self.report.index.name = 'date'
        self.report = self.report.T


@pytest.fixture(scope="module")
def oracle():
    return Oracle('app/tests/test-data_report-out.csv')


print(
    f'\n'
    f'init test/conftest'  # noqa
)
