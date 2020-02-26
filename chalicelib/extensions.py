
# ----------------------------------------------------------------------------------------------------------------------
#                                               EXCEPTIONS
# ----------------------------------------------------------------------------------------------------------------------


class ApplicationLogicException(Exception):
    """
    This exception is a parent exception for application logic exceptions.
    Exceptions of this type should be thrown, when something goes against of application logic.
    """
    def __init__(self, message):
        super().__init__(message or 'Application logic error!')


class ArgumentTypeException(TypeError):
    """ Specific TypeError for method arguments """

    def __init__(self, method_object, argument_name: str, argument_value):
        super().__init__('{0} {1} expects {2}, {3} is given!'.format(
            method_object.__qualname__,
            argument_name,
            method_object.__annotations__.get(argument_name, '???'),
            type(argument_value).__qualname__
        ))


class ArgumentValueException(ValueError):
    """ Base ValueError for argument values exceptions """
    pass


class ArgumentCannotBeEmptyException(ArgumentValueException):
    def __init__(self, method_object, argument_name: str):
        super().__init__('{0} {1} cannot be empty!'.format(method_object.__qualname__, argument_name))


class ArgumentUnexpectedValueException(ArgumentValueException):
    """ Specific ValueError, which should be thrown, when some value is out of range of known values """

    def __init__(self, value, supported_values: tuple):
        super().__init__('{0} is out of range of known values: {1}!'.format(value, supported_values))


# ----------------------------------------------------------------------------------------------------------------------


class HttpException(Exception):
    """ @deprecated - use built-in chalice exceptions """
    pass


class HttpAuthenticationRequiredException(HttpException):
    """ @deprecated - use built-in chalice exceptions """
    def __init__(self, message: str = None):
        super().__init__(message or 'Authentication is required!')


class HttpIncorrectInputDataException(HttpException):
    """ @deprecated - use built-in chalice exceptions """
    def __init__(self, message: str = None):
        super().__init__(message or 'Incorrect input data!')


class HttpNotFoundException(HttpException):
    """ @deprecated - use built-in chalice exceptions """
    def __init__(self, message: str = None):
        super().__init__(message or 'Not found!')


class HttpAccessDenyException(HttpException):
    """ @deprecated - use built-in chalice exceptions """
    def __init__(self, message: str = None):
        super().__init__(message or 'Access Denied!')


def http_response_exception_or_throw(e: BaseException) -> dict:
    """ @deprecated - use built-in chalice exceptions """
    if isinstance(e, HttpAuthenticationRequiredException):
        code = 'AuthenticationRequiredError'
    elif isinstance(e, HttpIncorrectInputDataException):
        code = 'IncorrectInputDataError'
    elif isinstance(e, ApplicationLogicException):
        code = 'ApplicationLogicError'
    elif isinstance(e, HttpNotFoundException):
        code = 'NotFoundError'
    elif isinstance(e, HttpAccessDenyException):
        code = 'AccessDenyError'
    else:
        # other not specified exception
        raise e

    return {
        'Code': code,
        'Message': str(e),
    }


# ----------------------------------------------------------------------------------------------------------------------
#                                               OBJECT FUNCTIONS
# ----------------------------------------------------------------------------------------------------------------------


def create_object(full_class_name: str, arguments: dict = None):
    if arguments and not isinstance(arguments, dict):
        raise TypeError('Function {} expects {} or None in "arguments", {} is given'.format(
            create_object.__qualname__,
            dict.__qualname__,
            arguments
        ))

    parts = full_class_name.split('.')

    module = __import__('.'.join(parts[:-1]))
    for submodule in parts[1:]:
        module = getattr(module, submodule)

    arguments = arguments or {}
    result = module(**arguments)

    return result


def clone(o):
    if o is None or type(o) is type:
        return o

    if type(o) in (bool, int, float, str):
        return type(o)(o)

    if type(o) in (list, tuple, set, frozenset):
        return type(o)([clone(i) for i in o])

    if type(o) is dict:
        o_clone = {}
        for key in o.keys():
            o_clone[key] = clone(o.get(key))
        return o_clone

    import datetime
    if isinstance(o, datetime.datetime) or isinstance(o, datetime.date):
        # datetime objects are immutable
        return o

    # custom objects
    o_clone = object.__new__(o.__class__)
    for property_name in tuple(o.__dict__.keys()):
        property_value = o.__dict__.get(property_name)
        cloned_property_value = clone(property_value)
        setattr(o_clone, property_name, cloned_property_value)

    return o_clone


# ----------------------------------------------------------------------------------------------------------------------

