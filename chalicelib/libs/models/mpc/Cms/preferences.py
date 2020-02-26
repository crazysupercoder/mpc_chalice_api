class Preference:
    __daily_email = True
    __weekly_email = False
    __alert_for_favorite_brand = False
    __sms = True

    def __init__(
            self,
            daily_email: bool=True,
            weekly_email: bool=False,
            alert_for_favorite_brand: bool=False,
            sms: bool=True, **kwargs):
        self.daily_email = daily_email
        self.weekly_email = weekly_email
        self.alert_for_favorite_brand = alert_for_favorite_brand
        self.sms = sms

    def to_dict(self):
        return {
            "daily_email": self.daily_email,
            "weekly_email": self.weekly_email,
            "alert_for_favorite_brand": self.alert_for_favorite_brand,
            "sms": self.sms,
        }

    @property
    def daily_email(self) -> bool:
        return self.__daily_email

    @daily_email.setter
    def daily_email(self, value: bool):
        self.__daily_email = value

    @property
    def weekly_email(self) -> bool:
        return self.__weekly_email

    @weekly_email.setter
    def weekly_email(self, value: bool):
        self.__weekly_email = value

    @property
    def alert_for_favorite_brand(self) -> bool:
        return self.__alert_for_favorite_brand

    @alert_for_favorite_brand.setter
    def alert_for_favorite_brand(self, value: bool):
        self.__alert_for_favorite_brand = value

    @property
    def sms(self) -> bool:
        return self.__sms

    @sms.setter
    def sms(self, value: bool):
        self.__sms = value

    @property
    def daily_email(self) -> bool:
        return self.__daily_email

    @daily_email.setter
    def daily_email(self, value: bool):
        self.__daily_email = value
