from app.core import settings
from app.core.output_to_exel import convert_to_xlsx_file
from app.core.parse_logs import parse_logs_files
from app.core.report_data import (
    aggregation_with_hours_counting,
    create_report_data,
)
from app.core.report_view import (
    report_to_csv_file,
    draw_graphs_on_total_connections,
    users_to_json_file,
    print_users_by_channels,
)


def main():
    print('init and start')

    data_from_logs = parse_logs_files(
        logs_path=settings.LOGS_PATH,
    )

    logs, users_by_canals = create_report_data(
        data_from_logs=data_from_logs,
        first_date=settings.FIRST_DATE,
        final_date=settings.FINAL_DATE,
    )

    print_users_by_channels(
        users_by_canals=users_by_canals
    )

    users_to_json_file(
        users=users_by_canals,
        file_name=settings.JSON_USERS_PATH,
    )

    report = aggregation_with_hours_counting(logs)

    print('\n')
    print(report)
    print('\n')

    report_to_csv_file(
        df=report,
        file_name=settings.CSV_REPORT_PATH
    )

    draw_graphs_on_total_connections(
        mikrotiks_logs=logs,
        users_by_canals=users_by_canals,
        max_con_file=settings.PNG_REPORT_MAX_CON_PATH,
        mean_con_file=settings.PNG_REPORT_MEAN_CON_PATH,
    )

    convert_to_xlsx_file(
        report,
        settings.XLSX_REPORT_PATH
    )


if __name__ == '__main__':
    main()
