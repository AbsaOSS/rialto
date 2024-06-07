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
import pytest

from rialto.jobs.decorators.resolver import Resolver, ResolverException


def test_simple_resolve_custom_name():
    def f():
        return 7

    Resolver.register_callable(f, "hello")

    assert Resolver.resolve("hello") == 7


def test_simple_resolve_infer_f_name():
    def f():
        return 7

    Resolver.register_callable(f)

    assert Resolver.resolve("f") == 7


def test_dependency_resolve():
    def f():
        return 7

    def g(f):
        return f + 1

    Resolver.register_callable(f)
    Resolver.register_callable(g)

    assert Resolver.resolve("g") == 8


def test_resolve_non_defined():
    with pytest.raises(ResolverException):
        Resolver.resolve("whatever")


def test_register_resolve(mocker):
    def f():
        return 7

    mocker.patch("rialto.jobs.decorators.resolver.Resolver.register_callable", return_value="f")
    mocker.patch("rialto.jobs.decorators.resolver.Resolver.resolve")

    Resolver.register_resolve(f)

    Resolver.register_callable.assert_called_once_with(f)
    Resolver.resolve.assert_called_once_with("f")
