import requests
import datetime
from typing import Optional
from chalicelib.extensions import *
from chalicelib.settings import Config
from chalicelib.libs.purchase.core import \
    Dtd, DtdCalculatorInterface, \
    SimpleSku, Qty, Name, Description


class _DtdApiCalculator(DtdCalculatorInterface):
    def __init__(self):
        self.__default_dtd_url = Config.DTD_API_DEFAULT_DTD_URL
        self.__default_dtd_min = Config.DTD_API_DEFAULT_DTD_MIN
        self.__default_dtd_max = Config.DTD_API_DEFAULT_DTD_MAX
        self.__sku_base_url = Config.DTD_API_SKU_BASE_URL

    def __get_default_dtd(self) -> Dtd:
        response = requests.get(self.__default_dtd_url)
        if response.status_code != 200:
            raise ValueError('Unable to get Default DTD! Service is unavailable!')

        content = response.json()
        if content.get('Error'):
            default_from_date = datetime.datetime.now() + datetime.timedelta(days=self.__default_dtd_min)
            default_to_date = datetime.datetime.now() + datetime.timedelta(days=self.__default_dtd_max)
            content = {
                'occasion': None,
                'data': {
                    'fromdtd_date': default_from_date.strftime('%Y-%m-%d'),
                    'todtd_date': default_to_date.strftime('%Y-%m-%d'),
                    'fromdtd': self.__default_dtd_min,
                    'todtd': self.__default_dtd_max,
                }
            }

        default_dtd = Dtd(
            Dtd.Occasion(
                Name(content.get('occasion').get('name')),
                Description(content.get('occasion').get('tooltip') or None)
            ) if content.get('occasion') and content.get('occasion').get('enabled') else None,
            datetime.date(
                int(content.get('data').get('fromdtd_date').split('-')[0]),
                int(content.get('data').get('fromdtd_date').split('-')[1]),
                int(content.get('data').get('fromdtd_date').split('-')[2])
            ),
            datetime.date(
                int(content.get('data').get('todtd_date').split('-')[0]),
                int(content.get('data').get('todtd_date').split('-')[1]),
                int(content.get('data').get('todtd_date').split('-')[2])
            ),
            int(content.get('data').get('fromdtd')),
            int(content.get('data').get('todtd'))
        )

        return default_dtd

    def __get_dtd_data(self, simple_sku_value: str) -> Optional[dict]:
        response = requests.get(self.__sku_base_url + simple_sku_value)
        if response.status_code != 200:
            raise ValueError('Unable to calculate DTD! Service is unavailable!')

        content = response.json()
        if content.get('Error'):
            return None

        return content

    def get_default(self) -> Dtd:
        return self.__get_default_dtd()

    def calculate(self, simple_sku: SimpleSku, qty: Qty) -> Dtd:
        if not isinstance(simple_sku, SimpleSku):
            raise ArgumentTypeException(self.calculate, 'simple_sku', simple_sku)
        if not isinstance(qty, Qty):
            raise ArgumentTypeException(self.calculate, 'qty', qty)

        content = self.__get_dtd_data(simple_sku.value)
        if not content:
            return self.__get_default_dtd()

        occasion = content.get('occasion')
        calculated_values = self.__get_values(content.get('data'), qty.value)
        if not calculated_values:
            return self.__get_default_dtd()

        result = Dtd(
            Dtd.Occasion(
                Name(occasion.get('name')),
                Description(occasion.get('tooltip') or None)
            ) if occasion and occasion.get('enabled') else None,
            calculated_values.get('date_from'),
            calculated_values.get('date_to'),
            calculated_values.get('working_days_from'),
            calculated_values.get('working_days_to')
        )

        return result

    def __get_values(self, dtd_data_items: list, ordered_qty: int) -> Optional[dict]:
        cpt = None
        jhb = None
        sw = None
        for dtd_data_item in dtd_data_items:
            location = dtd_data_item.get('location')
            if location == 'cpt':
                cpt = dtd_data_item
            elif location == 'jhb':
                jhb = dtd_data_item
            elif location == 'sw':
                sw = dtd_data_item
            else:
                raise ValueError('{} does not know, how to work with {} location!'.format(
                    self.__get_values,
                    location
                ))

        # Attention!
        # Currently we release products from CPT only, so JHB qty should be transferred to CPT first.
        # In this case logic is just "cpt or jhb or sw".

        result_wh_item = None
        cpt_qty = int(cpt.get('qty')) if cpt else 0
        jhb_qty = int(jhb.get('qty')) if jhb else 0
        sw_qty = int(sw.get('qty')) if sw else 0
        if cpt_qty >= ordered_qty:
            result_wh_item = cpt
        elif jhb_qty >= ordered_qty or cpt_qty + jhb_qty >= ordered_qty:
            result_wh_item = jhb
        elif sw_qty >= ordered_qty or cpt_qty + jhb_qty + sw_qty >= ordered_qty:
            result_wh_item = sw

        return {
            'date_from': datetime.date(
                int(result_wh_item.get('fromdtd_date').split('-')[0]),
                int(result_wh_item.get('fromdtd_date').split('-')[1]),
                int(result_wh_item.get('fromdtd_date').split('-')[2])
            ),
            'date_to': datetime.date(
                int(result_wh_item.get('todtd_date').split('-')[0]),
                int(result_wh_item.get('todtd_date').split('-')[1]),
                int(result_wh_item.get('todtd_date').split('-')[2])
            ),
            'working_days_from': int(result_wh_item.get('fromdtd')),
            'working_days_to': int(result_wh_item.get('todtd')),
        } if result_wh_item else None


# ----------------------------------------------------------------------------------------------------------------------


# instead of di-container, factories, etc.
class DtdCalculatorImplementation(DtdCalculatorInterface):
    def __init__(self):
        self.__calculator = _DtdApiCalculator()

    def get_default(self) -> Dtd:
        return self.__calculator.get_default()

    def calculate(self, simple_sku: SimpleSku, qty: Qty) -> Dtd:
        return self.__calculator.calculate(simple_sku, qty)


# ----------------------------------------------------------------------------------------------------------------------

