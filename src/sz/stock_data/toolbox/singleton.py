def singleton(class_):
    """
    example:

    @singleton
    class MyClass(BaseClass):
        pass

    """
    instances = {}

    def get_instance(*args, **kwargs):
        if class_ not in instances:
            instances[class_] = class_(*args, **kwargs)
        return instances[class_]

    return get_instance


class Singleton(object):
    """
    A Singleton object base class

    example:

    class MyClass(Singleton, BaseClass):
        pass

    warning:
        Multiple inheritance - eugh! __new__ could be overwritten during inheritance from a second base class?
        One has to think more than is necessary.
    """
    _instance = None

    def __new__(class_, *args, **kwargs):
        if not isinstance(class_._instance, class_):
            class_._instance = object.__new__(class_, *args, **kwargs)
        return class_._instance


class SingletonMeta(type):
    """
    通过 metaclass 的方式来实现单例
    例子:

    #Python2
    class MyClass(BaseClass):
        __metaclass__ = SingletonMeta

    #Python3
    class MyClass(BaseClass, metaclass=SingletonMeta):
        pass

    """

    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(SingletonMeta, cls).__call__(*args, **kwargs)
        return cls._instances[cls]
