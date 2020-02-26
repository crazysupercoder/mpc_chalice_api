from chalice import Blueprint, NotFoundError
from chalicelib.libs.models.mpc.Cms.StaticPages import StaticPageStorage


blueprint = Blueprint(__name__)


@blueprint.route('get/{descriptor}', methods=['GET'], cors=True)
def static_page_get(descriptor):
    static_page_storage = StaticPageStorage()

    descriptor = str(descriptor).strip() if descriptor else ''

    page = static_page_storage.get_by_descriptor(descriptor)
    if not page:
        raise NotFoundError('Page does not exist!')

    return {
        'name': page.name,
        'content': page.content
    }

