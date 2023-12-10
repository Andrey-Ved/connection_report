from app.core.parse_logs import parse_row_list, logs_to_df
from app.core.report_data import (
    aggregation_with_hours_counting,
    create_report_data,
)


def test(oracle):
    print(
        f'\n'
        f'start test'
        f'\n'
    )

    data_from_logs = logs_to_df(
        parse_row_list(
            oracle.raw_logs,
        )
    )

    logs, users_by_canals = create_report_data(
        data_from_logs=data_from_logs,
        first_date=oracle.first_date,
        final_date=oracle.final_date,
    )

    report = aggregation_with_hours_counting(logs)

    assert oracle.report.equals(report)
