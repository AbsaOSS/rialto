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

__all__ = ["get_pipelines_config", "transform_dependencies"]

from typing import Dict, List, Optional, Union

from pydantic import BaseModel

from rialto.common.utils import load_yaml


class IntervalConfig(BaseModel):
    units: str
    value: int


class ScheduleConfig(BaseModel):
    frequency: str
    day: Optional[int] = 0
    info_date_shift: Union[Optional[IntervalConfig], List[IntervalConfig]] = IntervalConfig(units="days", value=0)


class DependencyConfig(BaseModel):
    table: str
    name: Optional[str] = None
    date_col: Optional[str] = None
    interval: IntervalConfig


class ModuleConfig(BaseModel):
    python_module: str
    python_class: str


class MailConfig(BaseModel):
    sender: str
    to: List[str]
    smtp: str
    subject: str
    sent_empty: Optional[bool] = False


class GeneralConfig(BaseModel):
    target_schema: str
    target_partition_column: str
    source_date_column_property: Optional[str] = None
    watched_period_units: str
    watched_period_value: int
    job: str
    mail: MailConfig


class PipelineConfig(BaseModel):
    name: str
    module: Optional[ModuleConfig] = None
    schedule: ScheduleConfig
    dependencies: List[DependencyConfig] = []


class PipelinesConfig(BaseModel):
    general: GeneralConfig
    pipelines: list[PipelineConfig]


def get_pipelines_config(path) -> PipelinesConfig:
    """Load and parse yaml config"""
    return PipelinesConfig(**load_yaml(path))


def transform_dependencies(dependencies: List[DependencyConfig]) -> Dict:
    """Transform dependency config list into a dictionary"""
    res = {}
    for dep in dependencies:
        if dep.name:
            res[dep.name] = dep
    return res
