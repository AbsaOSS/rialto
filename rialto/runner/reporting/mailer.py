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

import smtplib
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import List

from rialto.runner.reporting.record import Record

__all__ = ["Mailer"]


class HTMLMessage:
    bck_colors = ["#00ded6", "#acfcfa"]
    borderless_table = 'role="presentation" style="border:0;border-spacing:0;"'
    bordered_table = (
        'role="presentation" style="background-repeat:no-repeat; margin:0;" cellpadding="1" cellspacing="1" border="1""'
    )

    @staticmethod
    def _get_status_color(status: str):
        if status == "Success":
            return "#398f00"
        elif status == "Error":
            return "#ff0000"
        else:
            return "#ff8800"

    @staticmethod
    def _make_rows(rows):
        html = ""
        data_options = 'align="center"'
        for row, i in zip(rows, range(len(rows))):
            r = f"""
                    <tr style="height:40px; margin:0; background-color:{HTMLMessage.bck_colors[i % 2]}">
                        <td {data_options}>{row.job}</td>
                        <td {data_options}>{row.target.split('.')[0]}.<br>
                                            {row.target.split('.')[1]}.<br>
                                            {row.target.split('.')[2]}
                        </td>
                        <td {data_options}>{row.date}</td>
                        <td {data_options}>{str(row.time).split(".")[0]}</td>
                        <td {data_options}>{f'{row.records:,}'}</td>
                        <td {data_options} style="color: {HTMLMessage._get_status_color(row.status)};">
                            <b>{row.status}</b>
                        </td>
                        <td {data_options}>{row.reason}</td>
                    </tr>
                """
            html += r
        return html

    @staticmethod
    def _make_overview_header():
        return """
            <tr style="height:40px; margin:2px; background-color: #286dd4;">
                <th>Job</th>
                <th>Target</th>
                <th>Date</th>
                <th>Time elapsed</th>
                <th>Rows created</th>
                <th>Status</th>
                <th>Reason</th>
            </tr>
        """

    @staticmethod
    def _make_header(start: datetime):
        return f"""
            <div align="center">
                <table {HTMLMessage.borderless_table}>
                    <tr>
                        <td align="center"><h3>This is a Rialto report<h3></td>
                    </tr>
                    <tr>
                        <td align="center">
                            Jobs started <b>{str(start).split('.')[0]}</b>
                        </td>
                    </tr>
                </table>
            </div>
        """

    @staticmethod
    def _make_overview(records: List[Record]):
        return f"""
            <table {HTMLMessage.borderless_table}>
                <tr>
                    <td><h3>Overview</h3></td>
                </tr>
            </table>
            <table {HTMLMessage.bordered_table}>
                {HTMLMessage._make_overview_header()}
                {HTMLMessage._make_rows(records)}
            </table>
        """

    @staticmethod
    def _head():
        return """
            <head>
                <meta charset="utf-8">
                <meta name="viewport" content="width=device-width,initial-scale=1">
                <meta name="x-apple-disable-message-reformatting">
                <!--[if !mso]><!-->
                    <meta http-equiv="X-UA-Compatible" content="IE=edge" />
                <!--<![endif]-->
                <!--[if mso]>
                    <noscript>
                        <xml>
                            <o:OfficeDocumentSettings>
                            <o:PixelsPerInch>96</o:PixelsPerInch>
                            </o:OfficeDocumentSettings>
                        </xml>
                    </noscript>
                <![endif]-->
            </head>
            <style>
                .foldingcheckbox { float: left; }
                .foldingcheckbox:not(:checked) + * { display: none }
            </style>
        """

    @staticmethod
    def _body_open():
        return """
            <body style="margin:0;padding:0;word-spacing:normal">
                <div role="article" aria-roledescription="email" lang="en"
                        style="-webkit-text-size-adjust:100%;-ms-text-size-adjust:100%;">
                    <div class="outer" style="width:100%;">
                """

    @staticmethod
    def _body_close():
        return """
                    </div>
                </div>
            </body>
        """

    @staticmethod
    def _make_exceptions(records: List[Record]):
        html = ""
        for record, i in zip(records, range(len(records))):
            if record.exception is not None:
                r = f"""
                    <table {HTMLMessage.bordered_table}>
                        <tr >
                            <td bgcolor={HTMLMessage.bck_colors[0]}>{record.job}</td>
                            <td bgcolor={HTMLMessage.bck_colors[1]}>{record.date}</td>
                        </tr>
                    </table>

                    <input class="foldingcheckbox" type="checkbox">Expand</input>
                    <div>
                        <table {HTMLMessage.borderless_table}>
                            <tr>
                                <td colspan="2">{record.exception}</td>
                            </tr>
                        </table>
                    </div>
                """
                html += r
        return html

    @staticmethod
    def _make_insights(records: List[Record]):
        return f"""
            <table {HTMLMessage.borderless_table}>
                <tr>
                    <td><h3>Exceptions<h3></td>
                </tr>
            </table>
            {HTMLMessage._make_exceptions(records)}
        """

    @staticmethod
    def make_report(start: datetime, records: List[Record]) -> str:
        """Create html email report"""
        html = [
            """<!DOCTYPE html>
            <html lang="en" xmlns="https://www.w3.org/1999/xhtml" xmlns:o="urn:schemas-microsoft-com:office:office">""",
            HTMLMessage._head(),
            HTMLMessage._body_open(),
            HTMLMessage._make_header(start),
            HTMLMessage._make_overview(records),
            HTMLMessage._make_insights(records),
            HTMLMessage._body_close(),
        ]
        return "\n".join(html)


class Mailer:
    """Send email reports"""

    @staticmethod
    def create_message(subject: str, sender: str, receiver: str, body: str) -> MIMEMultipart:
        """
        Create email message

        :param subject: email subject
        :param sender: email sender
        :param receiver: email receiver
        :param body: email body
        """
        msg = MIMEMultipart()
        msg["Subject"] = subject
        msg["From"] = sender
        msg["To"] = receiver
        body = MIMEText(body, "html")
        msg.attach(body)
        return msg

    @staticmethod
    def send_mail(smtp: str, message: MIMEMultipart):
        """
        Send email

        :param smtp: smtp server
        :param message: email message
        """
        s = smtplib.SMTP(host=smtp, port=25)
        s.sendmail(from_addr=message["From"], to_addrs=message["To"], msg=message.as_string())
        s.quit()
