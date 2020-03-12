from datetime import datetime, timedelta, timezone


DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'


def get_mpc_datetime_now(hours: int = 2) -> datetime:
    tz = timezone(timedelta(hours=hours), name="MPC Timezone")
    return datetime.now(tz=tz)
