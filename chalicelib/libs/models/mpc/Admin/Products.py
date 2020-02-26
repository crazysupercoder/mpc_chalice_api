import requests
from .....settings import settings
from decimal import Context as DecimalContext, ROUND_DOWN


class Products:
    def __init__(self, es_index_url, dynamo_table, auth=None):
        self.__awsauth = auth
        self.__es_index_url = es_index_url
        self.__dynamotbl = dynamo_table
        self.__doc_type = settings
        self.__headers = {"Content-Type": "application/json"}
        self.__must_be_decimal = ["selling_price", "rate"]
        self.__can_remove = ["supplier_product_name", "rs_sku", "product_size_attribute",
                             "supplier_sku", "product_description",
                             "rs_selling_price"]

        self.__currency_fields = ["selling_price"]

    def strip_blanks(self, data):
        remove = []
        for k in data:
            if data[k] == '' or data[k] is None:
                remove.append(k)

        for k in remove:
            del (data[k])

    def clean_values(self, data):
        for k in self.__must_be_decimal:
            if k in data:
                if data[k]:
                    strval = str(data[k])
                    strplit = strval.split('.')
                    if len(strplit) > 1:
                        strplit[1] = strplit[1][0:2]
                    strval = '.'.join(str(s) for s in strplit)
                    data[k] = strval
                    data[k] = float(data[k])

        return data

    def convert_decimal(self, data):
        context = DecimalContext(prec=10, rounding=ROUND_DOWN)
        for prices in data['prices']:
            for fld in self.__must_be_decimal:
                v = data['prices'][prices][fld]
                if v:
                    v = round(v, 2)
                    v = context.create_decimal_from_float(v)
                else:
                    v = context.create_decimal_from_float(0)
                data['prices'][prices][fld] = v
        return data

    def update_products_db(self, pdata):
        converteddata = self.convert_decimal(pdata)
        converteddata.update({
            'pk': converteddata['sku'],
            'sk': "PRODUCT"
        })
        self.strip_blanks(converteddata)
        return self.__dynamotbl.put_item(Item=pdata)

    def add_to_es(self, datadict):
        requests.put(self.__es_index_url + '/' + datadict['sku'],
                     json=datadict,
                     headers=self.__headers,
                     auth=self.__awsauth)

    def prepare_product(self, datadict):
        datadict.update({'selling_price': datadict['rs_selling_price']})
        datadict = self.clean_values(datadict)
        datadict = dict(datadict)
        datadict.update({'sku': datadict['rs_sku']})
        datadict.update({'product_type': datadict['product_size_attribute']})
        for rem in self.__can_remove:
            del (datadict[rem])

        base = dict()
        for cf in self.__currency_fields:
            base.update({cf: datadict[cf]})

        base.update({'currency': 'ZAR'})
        base.update({'rate': 1.0})
        base.update({'symbol': 'R'})
        currencies = {'ZAR': base}
        datadict.update({'prices': currencies})

        for rem in self.__must_be_decimal:
            if rem in datadict:
                del (datadict[rem])

        if 'sizes' in datadict:
            for size in datadict['sizes']:
                size.update({"size_tag": "{}|{}|{}|{}".format(
                    datadict['gender'],
                    datadict['product_type'],
                    datadict['manufacturer'],
                    size['size'])
                })

                size.update({"simple_size": "{}|{}".format(
                    datadict['product_type'],
                    size['size'])
                })

        self.add_to_es(datadict)

        sizes = {}
        if 'sizes' in datadict:
            for size in datadict['sizes']:
                sizes.update({size['rs_simple_sku']: size.copy()})

        datadict.update({'sizes': sizes})
        self.update_products_db(datadict)
