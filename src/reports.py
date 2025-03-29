import datetime as dt
import json
import logging
from collections.abc import Callable
from os import makedirs
from typing import Optional, ParamSpec

import pandas as pd

P = ParamSpec("P")

log_file = "logs/reports.log"
log_ok_str = "Выполнено без ошибок"
makedirs("logs", exist_ok=True)
logger = logging.getLogger(__name__)
file_formatter = logging.Formatter("%(asctime)s %(filename)s %(levelname)s: %(message)s")
file_handler = logging.FileHandler(log_file, "w", encoding="utf-8")
file_handler.setFormatter(file_formatter)
logger.addHandler(file_handler)
logger.setLevel(logging.DEBUG)


def write_report(
    filename: Optional[str] = None,
) -> Callable[[Callable[P, pd.DataFrame]], Callable[P, None]]:
    """Декоратор для записи отчета в файл в формате JSON"""

    def decorator(inner: Callable[P, pd.DataFrame]) -> Callable[P, None]:
        """Декоратор получает внутренюю функцию, которая должна возвращать DataFrame"""

        def wrapper(*args: P.args, **kwargs: P.kwargs) -> None:
            """wrapper получает результат внутренней функции и записывает его в файл JSON file"""

            report_filename = ""
            inner_name = inner.__name__
            if filename is None:
                date = dt.date.today().strftime("%Y-%m-%d")
                report_filename = f"data/{inner_name}_{date}.json"
            else:
                report_filename = filename
            inner_result = inner(*args, **kwargs)
            json_data = inner_result.to_dict("records")
            try:
                with open(report_filename, "w", encoding="utf-8") as f:
                    json.dump(json_data, f, ensure_ascii=False)
            except Exception as e:
                logger.error(f"decorator write_report был выполнен с ошибкой: {e}, функция {inner_name}")

        return wrapper

    return decorator


@write_report()
def spending_by_category(transactions: pd.DataFrame, category: str, date: Optional[str] = None) -> pd.DataFrame:
    """Создает отчет о расходах по категориям за 3 месяца"""

    filtered_df = pd.DataFrame()
    date_end = dt.date.today()
    if date is not None:
        try:
            date_end = dt.datetime.strptime(date, "%Y-%m-%d").date()
        except Exception as e:
            logger.warning(
                f"spending_by_category был выполнен с ошибкой: {e}, дата: {date}, использовалось текущая дата."
            )
    try:
        date_start = date_end - dt.timedelta(days=1)
        month_start = (date_start.month + 9) % 12
        date_start = date_start.replace(month=month_start)
        if date_start > date_end:
            year_start = date_end.year - 1
            date_start = date_start.replace(year=year_start)
        transactions["payment_date"] = pd.to_datetime(transactions["Дата платежа"], format="%d.%m.%Y").dt.date
    except Exception as e:
        logger.error(f"spending_by_category был выполнен с ошибкой: {e}")
        return filtered_df
    try:
        filtered_df = transactions.loc[
            (transactions["Статус"] == "OK")
            & (transactions["Сумма платежа"] < 0)
            & (transactions["Категория"] == category)
            & (transactions["payment_date"] <= date_end)
            & (transactions["payment_date"] >= date_start)
        ]
    except Exception as e:
        logger.error(f"spending_by_category был выполнен с ошибкой: {e}")
        return filtered_df
    logger.debug(f"spending_by_category {log_ok_str}")
    return filtered_df.drop(columns=["payment_date"])
