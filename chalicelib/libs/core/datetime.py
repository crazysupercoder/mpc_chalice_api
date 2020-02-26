from datetime import datetime, timedelta, timezone


def get_mpc_datetime_now() -> datetime:
    tz = timezone(timedelta(hours=2), name="MPC Timezone")
    return datetime.now(tz=tz)
