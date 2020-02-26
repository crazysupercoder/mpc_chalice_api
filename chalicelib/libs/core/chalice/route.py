from chalice.app import RouteEntry
from .config import MPCCORSConfig


class MPCRouteEntry(RouteEntry):
    def __init__(self, view_function, view_name, path, method,
            cors=False, **kwargs):
        super(MPCRouteEntry, self).__init__(view_function, view_name, path, method,
            cors=cors, **kwargs)
        if cors == True:
            self.cors = MPCCORSConfig()
