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

from rialto.jobs.resolver import Resolver, ResolverException


def test_simple_resolve_custom_name():
    def f():
        return 7

    resolver = Resolver()
    resolver.register_getter(f, "hello")

    assert resolver.resolve(lambda hello: hello) == 7


def test_simple_resolve_infer_f_name():
    def f():
        return 8

    resolver = Resolver()
    resolver.register_getter(f)

    assert resolver.resolve(lambda f: f) == 8


def test_resolve_non_defined():
    resolver = Resolver()
    with pytest.raises(ResolverException):
        resolver.resolve(lambda x: ...)


def test_resolve_multi_dependency():
    def a(b, c):
        return b + c

    def b():
        return 1

    def c(d):
        return d + 10

    def d():
        return 100

    resolver = Resolver()
    resolver.register_getter(a)
    resolver.register_getter(b)
    resolver.register_getter(c)
    resolver.register_getter(d)

    assert resolver.resolve(a) == 111


def test_register_objects():
    resolver = Resolver()
    resolver.register_object(7, "seven")
    assert resolver.resolve(lambda seven: seven) == 7
