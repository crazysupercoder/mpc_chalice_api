from chalice.app import (
    Chalice, _matches_content_type, CaseInsensitiveMapping,
    Response)
from .request import MPCRequest
from .route import MPCRouteEntry


def error_response(message, error_code, http_status_code, headers=None):
    body = {'Code': error_code, 'Message': message}
    response = Response(body=body, status_code=http_status_code,
                        headers=headers)

    return response.to_dict()


class MPCApi(Chalice):
    def _register_route(self, name, user_handler, kwargs, **unused):
        actual_kwargs = kwargs['kwargs']
        path = kwargs['path']
        url_prefix = kwargs.pop('url_prefix', None)
        if url_prefix is not None:
            path = '/'.join([url_prefix.rstrip('/'),
                             path.strip('/')])
        methods = actual_kwargs.pop('methods', ['GET'])
        route_kwargs = {
            'authorizer': actual_kwargs.pop('authorizer', None),
            'api_key_required': actual_kwargs.pop('api_key_required', None),
            'content_types': actual_kwargs.pop('content_types',
                                               ['application/json']),
            'cors': actual_kwargs.pop('cors', False),
        }
        if not isinstance(route_kwargs['content_types'], list):
            raise ValueError(
                'In view function "%s", the content_types '
                'value must be a list, not %s: %s' % (
                    name, type(route_kwargs['content_types']),
                    route_kwargs['content_types']))
        if actual_kwargs:
            raise TypeError('TypeError: route() got unexpected keyword '
                            'arguments: %s' % ', '.join(list(actual_kwargs)))
        for method in methods:
            if method in self.routes[path]:
                raise ValueError(
                    "Duplicate method: '%s' detected for route: '%s'\n"
                    "between view functions: \"%s\" and \"%s\". A specific "
                    "method may only be specified once for "
                    "a particular path." % (
                        method, path, self.routes[path][method].view_name,
                        name)
                )
            entry = MPCRouteEntry(user_handler, name, path, method,
                               **route_kwargs)
            self.routes[path][method] = entry

    def __call__(self, event, context):
        # This is what's invoked via lambda.
        # Sometimes the event can be something that's not
        # what we specified in our request_template mapping.
        # When that happens, we want to give a better error message here.
        resource_path = event.get('requestContext', {}).get('resourcePath')
        if resource_path is None:
            return error_response(error_code='InternalServerError',
                                  message='Unknown request.',
                                  http_status_code=500)
        http_method = event['requestContext']['httpMethod']
        if resource_path not in self.routes:
            raise ChaliceError("No view function for: %s" % resource_path)
        if http_method not in self.routes[resource_path]:
            return error_response(
                error_code='MethodNotAllowedError',
                message='Unsupported method: %s' % http_method,
                http_status_code=405)
        route_entry = self.routes[resource_path][http_method]
        view_function = route_entry.view_function
        function_args = {name: event['pathParameters'][name]
                         for name in route_entry.view_args}
        self.lambda_context = context
        self.current_request = MPCRequest(
            event['multiValueQueryStringParameters'],
            event['headers'],
            event['pathParameters'],
            event['requestContext']['httpMethod'],
            event['body'],
            event['requestContext'],
            event['stageVariables'],
            event.get('isBase64Encoded', False)
        )
        # We're getting the CORS headers before validation to be able to
        # output desired headers with
        cors_headers = None
        if self._cors_enabled_for_route(route_entry):
            cors_headers = self._get_cors_headers(route_entry.cors)
        # We're doing the header validation after creating the request
        # so can leverage the case insensitive dict that the Request class
        # uses for headers.
        if route_entry.content_types:
            content_type = self.current_request.headers.get(
                'content-type', 'application/json')
            if not _matches_content_type(content_type,
                                         route_entry.content_types):
                return error_response(
                    error_code='UnsupportedMediaType',
                    message='Unsupported media type: %s' % content_type,
                    http_status_code=415,
                    headers=cors_headers
                )
        response = self._get_view_function_response(view_function,
                                                    function_args)
        if cors_headers is not None:
            self._add_cors_headers(response, cors_headers)

        response_headers = CaseInsensitiveMapping(response.headers)
        if not self._validate_binary_response(
                self.current_request.headers, response_headers):
            content_type = response_headers.get('content-type', '')
            return error_response(
                error_code='BadRequest',
                message=('Request did not specify an Accept header with %s, '
                         'The response has a Content-Type of %s. If a '
                         'response has a binary Content-Type then the request '
                         'must specify an Accept header that matches.'
                         % (content_type, content_type)),
                http_status_code=400,
                headers=cors_headers
            )
        response = response.to_dict(self.api.binary_types)
        return response
