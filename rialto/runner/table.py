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

__all__ = ["Table"]

from typing import List

from rialto.metadata import class_to_catalog_name


class Table:
    """Handler for databricks catalog paths"""

    def __init__(
        self,
        catalog: str = None,
        schema: str = None,
        table: str = None,
        schema_path: str = None,
        table_path: str = None,
        class_name: str = None,
        partition: str = None,
        secondary_partitions: List[str] = None,
    ):
        self.catalog = catalog
        self.schema = schema
        self.table = table
        self.partition = partition
        self.secondary_partitions = secondary_partitions
        if schema_path:
            schema_path = schema_path.split(".")
            self.catalog = schema_path[0]
            self.schema = schema_path[1]
        if table_path:
            table_path = table_path.split(".")
            self.catalog = table_path[0]
            self.schema = table_path[1]
            self.table = table_path[2]
        if class_name and not table:
            self.table = class_to_catalog_name(class_name)

    def get_schema_path(self) -> str:
        """Get path of table's schema"""
        return f"{self.catalog}.{self.schema}"

    def get_table_path(self) -> str:
        """Get full table path"""
        return f"{self.catalog}.{self.schema}.{self.table}"

    def get_all_partitions(self) -> List[str]:
        """Get list of all partitions"""
        if self.secondary_partitions:
            return [self.partition] + self.secondary_partitions
        else:
            return [self.partition]
