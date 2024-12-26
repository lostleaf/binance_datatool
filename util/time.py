from datetime import date, timedelta

from dateutil import parser as date_parser


def convert_interval_to_timedelta(time_interval: str) -> timedelta:
    if time_interval.endswith('m') or time_interval.endswith('T'):
        return timedelta(minutes=int(time_interval[:-1]))

    if time_interval.endswith('H') or time_interval.endswith('h'):
        return timedelta(hours=int(time_interval[:-1]))

    raise ValueError('time_interval %s format error', time_interval)


def convert_date(dt) -> date:
    if isinstance(dt, str):
        return date_parser.parse(dt).date()
    return dt