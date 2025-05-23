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

__all__ = [
    "get_pipelines_config",
]

from typing import Dict, List, Optional

from pydantic import BaseModel, ConfigDict

from rialto.common.utils import load_yaml
from rialto.runner.config_overrides import override_config


class BaseConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")


class IntervalConfig(BaseConfig):
    units: str
    value: int


class ScheduleConfig(BaseConfig):
    frequency: str
    day: Optional[int] = 0
    info_date_shift: Optional[List[IntervalConfig]] = IntervalConfig(units="days", value=0)


class DependencyConfig(BaseConfig):
    table: str
    name: Optional[str] = None
    date_col: str
    interval: IntervalConfig


class ModuleConfig(BaseConfig):
    python_module: str
    python_class: str


class MailConfig(BaseConfig):
    sender: str
    to: List[str]
    smtp: str
    subject: str
    sent_empty: Optional[bool] = False


class RunnerConfig(BaseConfig):
    watched_period_units: str
    watched_period_value: int
    mail: Optional[MailConfig] = None
    bookkeeping: Optional[str] = None


class TargetConfig(BaseConfig):
    target_schema: str
    target_partition_column: str


class MetadataManagerConfig(BaseConfig):
    metadata_schema: str


class FeatureLoaderConfig(BaseConfig):
    feature_schema: str
    metadata_schema: str


class PipelineConfig(BaseConfig):
    name: str
    module: ModuleConfig
    schedule: ScheduleConfig
    dependencies: Optional[List[DependencyConfig]] = []
    target: TargetConfig = None
    metadata_manager: Optional[MetadataManagerConfig] = None
    feature_loader: Optional[FeatureLoaderConfig] = None
    extras: Optional[Dict] = {}


class PipelinesConfig(BaseConfig):
    runner: RunnerConfig
    pipelines: list[PipelineConfig]


def get_pipelines_config(path: str, overrides: Dict) -> PipelinesConfig:
    """Load and parse yaml config"""
    raw_config = load_yaml(path)
    if overrides:
        cfg = override_config(raw_config, overrides)
        return PipelinesConfig(**cfg)
    else:
        return PipelinesConfig(**raw_config)
