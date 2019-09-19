import threading
from functools import update_wrapper

__cache__ = dict()
__cache__lock__ = threading.Lock()


def lazy_fun(func):
    global __cache__

    def wrapper(*args, **kwds):
        value = __cache__.get(func, None)
        if value is None:
            with __cache__lock__:
                if not __cache__.__contains__(func):
                    v = func(*args, **kwds)
                    __cache__[func] = v
                    return v
                else:
                    return __cache__[func]
        else:
            return value

    return update_wrapper(wrapper, func)
