from __future__ import annotations

from analyze_step1_combined import main_combined
from contracts_amount import count_rows

# Governor windows used across analysis runs
GOVERNOR_WINDOWS: dict[str, tuple[str, str]] = {
    "Fortuño": ("2009-01-02", "2013-01-02"),
    "Padilla": ("2013-01-02", "2017-01-02"),
    "Rosello": ("2017-01-02", "2019-08-02"),
    "Pierluisi(De_Facto)": ("2019-08-02", "2019-08-07"),
    "Vazquez": ("2019-08-07", "2021-01-02"),
    "Pierluisi": ("2021-01-02", "2025-01-02"),
    "Gonzalez": ("2025-01-02", "2029-01-02"),
}

GOVERNORS = list(GOVERNOR_WINDOWS.keys())
METRICS = ["Contractors", "Service", "EntityName"]


def governor_dates(name: str) -> tuple[str, str]:
    return GOVERNOR_WINDOWS[name]


# Example usage for counting rows per governor
# for governor in GOVERNORS:
#     start, end = governor_dates(governor)
#     total = count_rows(start, end)
#     print(f"{total} rows for {governor}")


for governor in GOVERNORS:
    for metric in METRICS:
        start_date, end_date = governor_dates(governor)
        print(start_date, end_date)
        print(metric)
        print(governor)
        main_combined(start_date, end_date, governor, metric)
