from datetime import datetime, timedelta, timezone, date


def get_local_datetime():
    return datetime.now(timezone(timedelta(hours=5)))


def get_today_info():
    dt = get_local_datetime()
    today = dt.replace(hour=0, microsecond=0, minute=0, second=0)
    start_ts = int(today.timestamp())
    end_ts = start_ts + 86399
    return start_ts, end_ts, today

