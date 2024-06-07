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

__all__ = ["class_to_catalog_name"]


def class_to_catalog_name(class_name) -> str:
    """
    Map python class name of feature group (CammelCase) to databricks compatible format (lowercase with underscores)

    :param class_name: Python class name
    :return: feature storage name
    """
    res = []
    for i in range(0, len(class_name)):
        c = class_name[i]
        if c.isupper():
            if i != 0:
                res.append("_")
            res.append(c.lower())
        else:
            res.append(c)
    return "".join(res)
