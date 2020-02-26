from chalice import Blueprint
from ...libs.models.mpc.Cms.Banners import Banners
from ...libs.core.authorizer import cognito_authorizer

banners_blueprint = Blueprint(__name__)


@banners_blueprint.route('/add-banner', methods=['POST'], cors=True)
def addBanner():
    banners = Banners()
    request = banners_blueprint.current_request
    banners.insert(request.json_body)
    return {
        'result': 'success'
    }


@banners_blueprint.route('/update-banner/{banner_id}', methods=['PUT'], cors=True)
def updateBanner(banner_id):
    banners = Banners()
    request = banners_blueprint.current_request
    banners.update(banner_id, request.json_body)
    return {
        'result': 'success'
    }


@banners_blueprint.route('/get-banner/{banner_id}', cors=True)
def getBanner(banner_id):
    banners = Banners()
    item = banners.get(banner_id)
    return item


@banners_blueprint.route('/delete-banner/{banner_id}', cors=True)
def deleteBanner(banner_id):
    banners = Banners()
    banners.delete(banner_id)
    return {
        'result': 'success'
    }


@banners_blueprint.route('/list-banners', methods=['GET'], authorizer=cognito_authorizer, cors=True)
def listBanners():
    banners = Banners()
    response = banners.listAll()
    return response


@banners_blueprint.route('/list-banners-unauthed', methods=['GET'], cors=True)
def listBannersUnauthed():
    banners = Banners()
    response = banners.listAll()
    return response


@banners_blueprint.route('/list-banner', cors=True)
def listByQuery():
    banners = Banners()
    request = banners_blueprint.current_request
    queryList = {}
    if request.query_params is not None:
        if request.query_params.get('gender') is not None:
            queryList['gender']=request.query_params.get('gender')
    response = banners.list(queryList)
    return response

