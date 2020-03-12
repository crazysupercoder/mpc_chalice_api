from chalice.app import CORSConfig


class MPCCORSConfig(CORSConfig):
    _REQUIRED_HEADERS = [
        'Content-Type', 'X-Amz-Date', 'Authorization', 'X-Api-Key',
        'X-Amz-Security-Token', 'x-rws-token', 'rws-session-id',
        'email', 'first_name', 'last_name', 'id']
