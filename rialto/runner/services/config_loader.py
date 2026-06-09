#  Copyright 2022-2026 ABSA Group Limited
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
    "ConfigLoader",
]

from typing import Dict, List, Optional, Union

from pydantic import BaseModel, ConfigDict, Field

from rialto.common.utils import load_yaml
from rialto.runner.services.config_overrides import override_config


class BaseConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")


class IntervalConfig(BaseConfig):
    units: str
    value: int


class ScheduleConfig(BaseConfig):
    frequency: str
    day: Optional[Union[int, str]] = 0
    info_date_shift: Optional[Union[IntervalConfig, List[IntervalConfig]]] = Field(
        default_factory=lambda: IntervalConfig(units="days", value=0)
    )


class DependencyConfig(BaseConfig):
    table: str
    name: Optional[str] = None
    date_col: str
    interval: IntervalConfig
    filters: Optional[Dict] = None


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
    secondary_partition_columns: Optional[List[str]] = None
    rerun_filters: Optional[Dict] = None
    custom_name: Optional[str] = None


class MetadataManagerConfig(BaseConfig):
    metadata_schema: str


class FeatureLoaderConfig(BaseConfig):
    feature_schema: str
    metadata_schema: str


class PipelineConfig(BaseConfig):
    name: str
    module: ModuleConfig
    schedule: ScheduleConfig
    dependencies: Optional[List[DependencyConfig]] = Field(default_factory=list)
    target: Optional[TargetConfig] = None
    metadata_manager: Optional[MetadataManagerConfig] = None
    feature_loader: Optional[FeatureLoaderConfig] = None
    extras: Optional[Dict] = Field(default_factory=dict)


class PipelinesConfig(BaseConfig):
    runner: RunnerConfig
    pipelines: List[PipelineConfig]


def get_pipelines_config(path: str, overrides: Dict) -> PipelinesConfig:
    """Load and parse yaml config"""
    raw_config = load_yaml(path)
    if overrides:
        cfg = override_config(raw_config, overrides)
        return PipelinesConfig(**cfg)
    else:
        return PipelinesConfig(**raw_config)


class ConfigLoader:
    """Loader for pipelines config"""

    @staticmethod
    def load_yaml(path: str, overrides: Dict) -> PipelinesConfig:
        """Load yaml config and apply overrides"""
        return get_pipelines_config(path, overrides)
