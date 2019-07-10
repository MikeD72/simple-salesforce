"""Utility functions for simple-salesforce"""

import xml.dom.minidom
import csv
import io
from itertools import chain

from simple_salesforce.exceptions import (
    SalesforceGeneralError, SalesforceExpiredSession,
    SalesforceMalformedRequest, SalesforceMoreThanOneRecord,
    SalesforceRefusedRequest, SalesforceResourceNotFound
)


# pylint: disable=invalid-name
def getUniqueElementValueFromXmlString(xmlString, elementName):
    """
    Extracts an element value from an XML string.

    For example, invoking
    getUniqueElementValueFromXmlString(
        '<?xml version="1.0" encoding="UTF-8"?><foo>bar</foo>', 'foo')
    should return the value 'bar'.
    """
    xmlStringAsDom = xml.dom.minidom.parseString(xmlString)
    elementsByName = xmlStringAsDom.getElementsByTagName(elementName)
    elementValue = None
    if len(elementsByName) > 0:
        elementValue = elementsByName[0].toxml().replace(
            '<' + elementName + '>', '').replace('</' + elementName + '>', '')
    return elementValue


def date_to_iso8601(date):
    """Returns an ISO8601 string from a date"""
    datetimestr = date.strftime('%Y-%m-%dT%H:%M:%S')
    timezone_sign = date.strftime('%z')[0:1]
    timezone_str = '%s:%s' % (
        date.strftime('%z')[1:3], date.strftime('%z')[3:5])
    return '{datetimestr}{tzsign}{timezone}'.format(
        datetimestr=datetimestr,
        tzsign=timezone_sign,
        timezone=timezone_str
        ).replace(':', '%3A').replace('+', '%2B')


def exception_handler(result, name=""):
    """Exception router. Determines which error to raise for bad results"""
    try:
        response_content = result.json()
    # pylint: disable=broad-except
    except Exception:
        response_content = result.text

    exc_map = {
        300: SalesforceMoreThanOneRecord,
        400: SalesforceMalformedRequest,
        401: SalesforceExpiredSession,
        403: SalesforceRefusedRequest,
        404: SalesforceResourceNotFound,
    }
    exc_cls = exc_map.get(result.status_code, SalesforceGeneralError)

    raise exc_cls(result.url, result.status_code, name, response_content)


def call_salesforce(url, method, session, headers, **kwargs):
    """Utility method for performing HTTP call to Salesforce.

    Returns a `requests.result` object.
    """

    additional_headers = kwargs.pop('additional_headers', dict())
    headers.update(additional_headers or dict())
    result = session.request(method, url, headers=headers, **kwargs)

    if result.status_code >= 300:
        exception_handler(result)

    return result


def json_list_to_file(data):
    """Returns a CSV from a list of json objects"""
    output = io.StringIO(newline=None) # newlines are translated to the system default line separator, os.linesep
    sentinel = 0
    for row in data:
        if sentinel == 0:
            writer = csv.DictWriter(output, fieldnames=row.keys(), 
                                    delimiter=',', quotechar='"',
                                    quoting=csv.QUOTE_MINIMAL)
            writer.writeheader()
            sentinel += 1

        writer.writerow(row)
    output.seek(0)
    return output


def split_csv(data, split_size=100):
    """Returns a list of CSVs that are under the split_size

        Arguments:

        * data -- CSV file of data
        * split_size -- maximum size of each chunk, in MB. Bulk API v2 cannot exceed 100MB in utf-8 encoding"""
    from timeit import default_timer
    max_bytes = int(split_size * 1024 * 1024)
    header = next(data)
    for line in data:
        start = default_timer()
        output = io.StringIO(newline=None) # newlines are translated to the system default line separator, os.linesep
        writer = csv.writer(output,
                            delimiter=',', quotechar='"',
                            quoting=csv.QUOTE_MINIMAL)
        writer.writerow(header)
        n = 0
        log_rows = 0
        for line in chain([line], data):
            writer.writerow(line)
            n += sum([len(item)+3 for item in line]) + 1
            log_rows += 1
            if n >= max_bytes:
                duration = default_timer()-start 
                print(f'split out {n/1024/1024:8.8} MBs ({log_rows:} rows) in {duration:4.4} seconds')
                yield output.getvalue()
                break
    duration = default_timer()-start 
    print(f'split out {n/1024/1024:8.8} MBs ({log_rows:} rows) in {duration:4.4} seconds')
    yield output.getvalue()
