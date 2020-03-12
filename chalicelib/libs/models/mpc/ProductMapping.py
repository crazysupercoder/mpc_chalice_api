mapping = {
    "mappings": {
        "products": {
            "properties": {
                "portal_config_id": {"type": "integer"},
                "rs_sku": {"type": "keyword"},
                "event_code": {"type": "keyword"},

                # config attributes 1
                "manufacturer": {"type": "keyword"},
                "product_size_attribute": {"type": "keyword"},  # product type name
                "rs_product_sub_type": {"type": "keyword"},
                "rs_colour": {"type": "keyword"},
                "gender": {"type": "keyword"},
                "season": {"type": "keyword"},

                # config attributes 2
                "size_chart": {"type": "keyword"},
                "neck_type": {"type": "keyword"},
                "fit": {"type": "keyword"},
                "dimensions": {"type": "keyword"},
                "fabrication": {"type": "keyword"},
                "size_fit": {"type": "keyword"},
                "sticker_id": {"type": "long"},

                # name
                "product_name": {"type": "text"},
                "product_description": {"type": "text"},

                # prices
                "freebie": {"type": "boolean"},
                "rs_selling_price": {"type": "float"},
                "discount": {"type": "float"},  # percent value

                # datetime
                "created_at": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss||date_hour_minute_second_millis"},
                "updated_at": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss||date_hour_minute_second_millis"},

                # simples
                "sizes": {
                    "properties": {
                        "size": {"type": "keyword"},
                        "qty": {"type": "integer"},
                        "rs_simple_sku": {"type": "keyword"},
                        "portal_simple_id": {"type": "integer"}
                    }
                },

                # Next fields need to be checked
                # ------------------------------

                # @todo : lowercase "manufacturer"
                "brand_code": {"type": "keyword"},

                # @todo : looks like is not used
                "status": {"type": "integer"},

                # @todo : "images" has the same data as in the "img" property, but in other structure.
                # "img" property has difficult structure to describe it in the mapping. Needs to be refactored first.
                # "img": {
                #     "properties": {
                #         "images": {
                #             "properties": {
                #                 "back": {"type": "text"},
                #                 "lifestyle": {"type": "text"},
                #                 "small": {"type": "text"}
                #             }
                #         },
                #         "media_gallery": {
                #             "properties": {
                #                 for i in range(0, n)
                #                   "i": {"type": "text"}
                #             }
                #         }
                #     }
                # },
                "images": {
                    "properties": {
                        "s3_filepath": {"type": "keyword"},
                        "position": {"type": "keyword"},
                        "delete": {"type": "integer"}   # @todo : looks like is not needed
                    }
                }
            }
        }
    }
}

tracking_counters = {
    "mappings": {
        "products_tracking_counters": {
            "properties": {
                "config_sku": {"type": "keyword"},
                "views": {"type": "integer"},
                "visits": {"type": "integer"}
            }
        }
    }
}

tracking_user_action = {
    "mappings": {
        "products_tracking_user_action": {
            "properties": {
                "config_sku": {"type": "keyword"},
                "snapshot_id": {"type": "keyword"},
                "session_id": {"type": "keyword"},
                "user_id": {"type": "keyword"},
                "user_tier": {
                    "properties": {
                        "name": {"type": "keyword"},
                        "discount_rate": {"type": "integer"}
                    }
                },
                "action": {"type": "keyword"},
                "action_at": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"}
            }
        }
    }
}

tracking_user_action_product_snapshot = {
    "mappings": {
        "products_tracking_user_action_product_snapshot": {
            "properties": {
                "snapshot_id": {"type": "keyword"},
                "config_sku": {"type": "keyword"},
                "updated_at": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"},
                "data": {
                    # similar to product, but not the same!
                    "properties": {
                        "event_code": {"type": "keyword"},

                        # config attributes 1
                        "manufacturer": {"type": "keyword"},
                        "product_size_attribute": {"type": "keyword"},  # product type name
                        "rs_product_sub_type": {"type": "keyword"},
                        "rs_colour": {"type": "keyword"},
                        "gender": {"type": "keyword"},
                        "season": {"type": "keyword"},

                        # config attributes 2
                        "size_chart": {"type": "keyword"},
                        "neck_type": {"type": "keyword"},
                        "fit": {"type": "keyword"},
                        "dimensions": {"type": "keyword"},
                        "fabrication": {"type": "keyword"},
                        "size_fit": {"type": "keyword"},
                        "sticker_id": {"type": "long"},

                        # name
                        "product_name": {"type": "text"},
                        "product_description": {"type": "text"},

                        # prices
                        "freebie": {"type": "boolean"},
                        "rs_selling_price": {"type": "float"},
                        "discount": {"type": "float"},  # percent value

                        # @todo : looks like is not used
                        "status": {"type": "integer"},

                        # @todo : can be just json array
                        "sizes": {
                            "properties": {
                                "size": {"type": "keyword"},
                                "rs_simple_sku": {"type": "keyword"},
                            }
                        },

                        # @todo : can be just json array. Check products mapping also.
                        "images": {
                            "properties": {
                                "s3_filepath": {"type": "keyword"},
                                "position": {"type": "keyword"}
                            }
                        }
                    }
                }
            }
        }
    }
}


scored_products_mapping = {
    "mappings": {
        "scored_products": {
            "properties": {
                'customer_id': {'type': 'keyword'},
                'question_score': {'type': 'float'},
                'order_score': {'type': 'float'},
                'tracking_score': {'type': 'float'},
                **mapping['mappings']['products']['properties'],
                'views': {'type': 'integer'},
                'clicks': {'type': 'integer'},
                'visits': {'type': 'integer'},
                'viewed_at': {"type": "date", "format": "yyyy-MM-dd HH:mm:ss||date_hour_minute_second_millis"}
            }
        }
    }
}
