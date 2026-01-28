#  Copyright 2022 ABSA Group Limited
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

__all__ = ["feature", "desc", "param", "depends"]

import typing
from copy import deepcopy
from functools import partial, wraps

from loguru import logger

from rialto.maker.containers import FeatureFunction, FeatureHolder
from rialto.metadata import ValueType


def decorator_with_args(decorator_to_enhance):
    """
    Wrap decorators to use args

    This function is supposed to be used as a decorator.
    It must decorate and other function, that is intended to be used as a decorator.
    It will allow any decorator to accept an arbitrary number of arguments
    """

    @wraps(decorator_to_enhance)
    def decorator_maker(*args, **kwargs):
        """
        Pass arguments to inner wrapper

        We create on the fly a decorator that accepts only a function
        but keeps the passed arguments from the maker.
        """

        def decorator_wrapper(func):
            """
            Pass arguments to the wrapped function

            We return the result of the original decorator
            the decorator must have this specific signature otherwise it won't work:
            """
            return decorator_to_enhance(func, *args, **kwargs)

        return decorator_wrapper

    return decorator_maker


@decorator_with_args
def feature(feature_functions: typing.Union[typing.Callable, FeatureHolder], feature_type: ValueType):
    """
    Wrap a feature definitions. Use as the outermost decorator!!!

    The purpose of this wrapper is to wrap function that have no other decorator inside FeatureHolder
    and define feature type
    :param feature_functions: FeatureHolder or pure function
    :param feature_type: FeatureType enum (numerical, ordinal, nominal)
    :return: FeatureHolder
    """
    logger.trace(f"Wrapping {feature_functions} as feature")

    def wrapper() -> FeatureHolder:
        if isinstance(feature_functions, FeatureHolder):
            for f in feature_functions:
                f.type = feature_type
            return feature_functions
        else:
            func_list = FeatureHolder()
            new_feature_f = FeatureFunction(feature_functions.__name__, feature_functions, feature_type)
            func_list.append(new_feature_f)
            return func_list

    return wrapper()


@decorator_with_args
def desc(feature_functions: typing.Union[typing.Callable, FeatureHolder], desc: str):
    """
    Wrap feature with string description, used in metadata

    :param feature_functions: FeatureHolder or pure function
    :param type: FeatureType enum (numerical, ordinal, nominal)
    :return: FeatureHolder
    """
    logger.trace(f"Wrapping {feature_functions} with description")

    def wrapper() -> FeatureHolder:
        if isinstance(feature_functions, FeatureHolder):
            for f in feature_functions:
                f.description = desc
            return feature_functions
        else:
            func_list = FeatureHolder()
            new_feature_f = FeatureFunction(feature_functions.__name__, feature_functions)
            new_feature_f.description = desc
            func_list.append(new_feature_f)
            return func_list

    return wrapper()


@decorator_with_args
def param(feature_functions: typing.Union[typing.Callable, FeatureHolder], parameter_name: str, values: typing.List):
    """
    Wrap a feature function with custom parameter, can be chained

    Creates a cartesian product of all previous parameters with new ones
    :param feature_functions: FeatureHolder or pure function
    :param parameter_name: string name of the parameter
    :param values: a list of parameter values
    :return: FeatureHolder
    """
    logger.trace(f"Parametrize \n\tfunc: {feature_functions}\n\tparam:{parameter_name}\n\tvalues{values}")

    def expander() -> FeatureHolder:
        func_list = FeatureHolder()
        if isinstance(feature_functions, FeatureHolder):
            for f in feature_functions:
                for value in values:
                    func_list.append(_copy_with_new_parameter(f, parameter_name, value))
        else:
            for value in values:
                func_list.append(_create_new_with_parameter(feature_functions, parameter_name, value))
        return func_list

    return expander()


def _copy_with_new_parameter(func: FeatureFunction, parameter: str, value: typing.Any) -> FeatureFunction:
    """
    Create a deep copy of FeatureFunction and store new parameter in it

    :param func: FeatureFunction
    :param parameter: string parameter name
    :param value: any parameter value
    :return: FeatureFunction
    """
    logger.trace(f"Extending \n\tfunction: {func} \n\tby parameter: {parameter} \n\twith value: {value}")
    new_feature_f = deepcopy(func)
    new_feature_f.callable = partial(func.callable, **{parameter: value})
    new_feature_f.parameters[parameter] = value
    return new_feature_f


def _create_new_with_parameter(func: typing.Callable, parameter: str, value: typing.Any) -> FeatureFunction:
    """
    Create a new FeatureFunction from passed callable and store new parameter in it

    :param func: callable object
    :param parameter: string parameter name
    :param value: any parameter value
    :return: FeatureFunction
    """
    logger.trace(f"Creating \n\tfunction: {func} \n\twith parameter: {parameter} \n\twith value: {value}")
    new_feature_f = FeatureFunction(func.__name__, partial(func, **{parameter: value}))
    new_feature_f.parameters[parameter] = value
    return new_feature_f


@decorator_with_args
def depends(feature_functions: typing.Union[typing.Callable, FeatureHolder], dependency: str):
    """
    Specify dependency features

    The purpose of this wrapper is to define inter-feature dependency and order the execution accordingly
    :param feature_functions: FeatureHolder or pure function
    :param dependency: str name of required function
    :return: FeatureHolder
    """
    logger.trace(f"Wrapping {feature_functions} with dependency {dependency}")

    def wrapper() -> FeatureHolder:
        if isinstance(feature_functions, FeatureHolder):
            for f in feature_functions:
                f.dependencies.append(dependency)
            return feature_functions
        else:
            func_list = FeatureHolder()
            new_feature_f = FeatureFunction(feature_functions.__name__, feature_functions)
            new_feature_f.dependencies.append(dependency)
            func_list.append(new_feature_f)
            return func_list

    return wrapper()
