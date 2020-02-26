from ...libs.core.trackers import EventTracker as Recorder


def register_event_trackers(blue_print):
    @blue_print.route('/record', methods=['POST'], cors=True)
    def track_events():
        request = blue_print.current_request
        customer_id = request.email
        session_id = request.session_id
        if request.json_body is None:
            return {"msg": "Missing required parameters."}

        product_type_id = request.json_body.get('product_type_id')
        rs_config_sku = request.json_body.get('rs_config_sku')
        rs_simple_sku = request.json_body.get('rs_simple_sku')
        gender = request.json_body.get('gender')
        response = Recorder.track(
            customer_id, session_id,
            simple_sku=rs_simple_sku, config_sku=rs_config_sku,
            product_type=product_type_id, gender=gender)
        return {"result": response}
