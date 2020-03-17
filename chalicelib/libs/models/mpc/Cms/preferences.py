import re
from typing import List, Union, Any
from chalicelib.libs.core.sqs_sender import SqsSenderEventInterface


class PreferenceOption:
    """Value options of an individual communication
    """
    none: str = 'none'
    very_little: str = 'very_little'
    less: str = 'less'
    normal: str = 'normal'

    @staticmethod
    def lower(value: str) -> str:
        value = re.sub(r'\s+', ' ', value).strip().lower()
        return '_'.join(value.split(' '))

    @staticmethod
    def capitalize(value: str) -> str:
        value = re.sub(r'\_+', '_', value).strip().lower()
        return ' '.join([item.capitalize() for item in value.split('_') if item])


class Preference(object):
    """Notification Preferences settings entry class
    - on_site_popups
    - emails
    - off_site_notifications
    """
    __attributes__ = ('on_site_popups', 'emails', 'off_site_notifications')

    def __init__(
        self,
        on_site_popups: str = None,
        emails: str = None,
        off_site_notifications: str = None,
        alert_for_favorite_brand: int = 0,
        **kwargs
    ):
        self.on_site_popups = on_site_popups
        self.emails = emails
        self.off_site_notifications = off_site_notifications
        self.alert_for_favorite_brand = alert_for_favorite_brand

    def __setattr__(self, name: str, value: Any):
        if name in self.__class__.__attributes__:
            if not isinstance(value, str):
                value = str(value)
            if hasattr(PreferenceOption, PreferenceOption.lower(value)):
                value = getattr(PreferenceOption, PreferenceOption.lower(value))
            else:
                value = PreferenceOption.none
        super(Preference, self).__setattr__(name, value)

    def to_dict(self) -> dict:
        params = dict([
            (item, PreferenceOption.capitalize(getattr(self, item)))
            for item in self.__attributes__])
        params['alert_for_favorite_brand'] = self.alert_for_favorite_brand
        return params


class CommunicationPreferencesUpdateSqsSenderEvent(SqsSenderEventInterface):
    def __init__(
        self,
        email: str,
        on_site_popups: str,
        emails: str,
        off_site_notifications: str,
        alert_for_favorite_brand: int = 0
    ):
        self.email = email
        self.on_site_popups = PreferenceOption.lower(on_site_popups)
        self.emails = PreferenceOption.lower(emails)
        self.off_site_notifications = PreferenceOption.lower(off_site_notifications)
        self.alert_for_favorite_brand = alert_for_favorite_brand

    @classmethod
    def _get_event_type(cls) -> str:
        return 'communication_preferences'

    @property
    def event_data(self) -> dict:
        return {
            'email': self.email,
            'on_site_popups': self.on_site_popups,
            'emails': self.emails,
            'off_site_notifications': self.off_site_notifications,
            'alert_for_favorite_brand': self.alert_for_favorite_brand,
        }

    def __repr__(self) -> str:
        return (
            f"<{self.email}: ({self.on_site_popups}, {self.emails}, "
            f"{self.off_site_notifications}, {self.alert_for_favorite_brand})>"
        )
