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

__all__ = ["Tracker"]

from datetime import datetime

from pyspark.sql import SparkSession

from rialto.runner.config_loader import MailConfig
from rialto.runner.reporting.bookkeeper import BookKeeper
from rialto.runner.reporting.mailer import HTMLMessage, Mailer
from rialto.runner.reporting.record import Record


class Tracker:
    """Collect information about runs and sent them out via email"""

    def __init__(self, mail_cfg: MailConfig, bookkeeping: str = None, spark: SparkSession = None):
        self.records = []
        self.last_error = None
        self.pipeline_start = datetime.now()
        self.exceptions = []
        self.mail_cfg = mail_cfg
        self.bookkeeper = None

        if bookkeeping:
            self.bookkeeper = BookKeeper(table=bookkeeping, spark=spark)

    def add(self, record: Record) -> None:
        """Add record for one run"""
        self.records.append(record)
        if self.bookkeeper:
            self.bookkeeper.add(record)

    def report_by_mail(self):
        """Create and send html report"""
        if self.mail_cfg is None:
            return
        if len(self.records) or self.mail_cfg.sent_empty:
            report = HTMLMessage.make_report(self.pipeline_start, self.records)
            for receiver in self.mail_cfg.to:
                message = Mailer.create_message(
                    subject=self.mail_cfg.subject, sender=self.mail_cfg.sender, receiver=receiver, body=report
                )
                Mailer.send_mail(self.mail_cfg.smtp, message)
