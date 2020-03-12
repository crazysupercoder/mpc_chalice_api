from chalice import Blueprint
from chalicelib.libs.purchase.core import CustomerTier
from chalicelib.libs.purchase.customer.storage import CustomerTierStorageImplementation


def register_tiers(blueprint: Blueprint):
    @blueprint.route('/tiers/list', methods=['GET'], cors=True)
    def tiers_list():
        customer_tiers_storage = CustomerTierStorageImplementation()

        def __sort_tiers(tier: CustomerTier):
            return tier.credit_back_percent.value

        tiers = list(customer_tiers_storage.get_all())
        tiers.sort(key=__sort_tiers)

        return [{
            'name': tier.name.value,
            'credit_back_percent': tier.credit_back_percent.value,
        } for tier in tiers if not tier.is_neutral]

