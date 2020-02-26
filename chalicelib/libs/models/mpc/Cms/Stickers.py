from .....settings import settings
from ..base import DynamoModel


class StickerEntity:

    def __init__(self, sticker_id: int, name: str, image_url: str = None):
        if sticker_id <= 0:
            raise ValueError(self.__class__.__name__ + ' "id" value is incorrect!')

        if len(name) == 0:
            raise ValueError(self.__class__.__name__ + ' "name" is required!')

        image_url = str(image_url).strip()
        image_url = image_url if image_url not in [None, 'None'] and len(image_url) > 0 else None

        self.__id = sticker_id
        self.__name = name
        self.__image_url = image_url

    @property
    def id(self):
        return self.__id

    @property
    def name(self):
        return self.__name

    @property
    def image_url(self):
        return self.__image_url


# ----------------------------------------------------------------------------------------------------------------------


class StickerModel(DynamoModel):
    TABLE_NAME = settings.AWS_DYNAMODB_CMS_TABLE_NAME
    PARTITION_KEY = 'STICKER'

    def __init__(self):
        super(StickerModel, self).__init__(self.TABLE_NAME)

    def save(self, sticker: StickerEntity) -> None:
        sticker_data = {
            'pk': self.get_partition_key(),
            'sk': str(sticker.id),
            'name': str(sticker.name),
            'image_url': sticker.image_url,
        }

        # insert or update
        self.table.put_item(Item=sticker_data)

    def delete(self, sticker_id: int) -> None:
        key = {
            'pk': self.get_partition_key(),
            'sk': str(sticker_id),
        }

        self.table.delete_item(Key=key)


# ----------------------------------------------------------------------------------------------------------------------

