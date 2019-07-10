""" Classes for interacting with Salesforce Bulk API """

try:
    from collections import OrderedDict
except ImportError:
    # Python < 2.7
    from ordereddict import OrderedDict

import json
import requests
from time import sleep
from simple_salesforce.util import (
      call_salesforce
    , json_list_to_file
    , split_csv
)
import csv

class SFBulk_v2Handler(object):
    """ Bulk API request handler
    Intermediate class which allows us to use commands,
     such as 'sf.bulk.Contacts.insert(...)'
    This is really just a middle layer, whose sole purpose is
    to allow the above syntax
    """

    def __init__(self, session_id, bulk_url, proxies=None, session=None):
        """Initialize the instance with the given parameters.

        Arguments:

        * session_id -- the session ID for authenticating to Salesforce
        * bulk_url -- API endpoint set in Salesforce instance
        * proxies -- the optional map of scheme to proxy server
        * session -- Custom requests session, created in calling code. This
                     enables the use of requests Session features not otherwise
                     exposed by simple_salesforce.
        """
        self.session_id = session_id
        self.session = session or requests.Session()
        self.bulk_url = bulk_url
        # don't wipe out original proxies with None
        if not session and proxies is not None:
            self.session.proxies = proxies

        # Define these headers separate from Salesforce class,
        # as bulk uses a slightly different format
        self.headers = {
            'Content-Type': 'application/json; charset=UTF-8',
            'Accept': 'application/json',
            'Authorization': 'Bearer ' + self.session_id
        }

    def __getattr__(self, name):
        return SFBulk_v2Type(object_name=name, bulk_url=self.bulk_url,
                          headers=self.headers, session=self.session)

class SFBulk_v2Type(object):
    """ Interface to Bulk/Async API functions"""

    def __init__(self, object_name, bulk_url, headers, session):
        """Initialize the instance with the given parameters.

        Arguments:

        * object_name -- the name of the type of SObject this represents,
                         e.g. `Lead` or `Contact`
        * bulk_url -- API endpoint set in Salesforce instance
        * headers -- bulk API headers
        * session -- Custom requests session, created in calling code. This
                     enables the use of requests Session features not otherwise
                     exposed by simple_salesforce.
        """
        self.object_name = object_name
        self.bulk_url = bulk_url
        self.session = session
        self.headers = headers

    def _create_job(self, operation, object_name, external_id_field=None):
        """ Create a bulk job

        Arguments:

        * operation -- Bulk operation to be performed by job
        * object_name -- SF object
        * external_id_field -- unique identifier field for upsert operations
        """
        
        # if os.linesep=='\r\n':
        #     lineEnding = 'CRLF'
        # else:
        #     lineEnding = 'LF'
            
        payload = {
            'operation': operation,
            'object': object_name,
            'contentType': 'CSV',
            'lineEnding': 'LF'
        }

        if operation == 'upsert':
            payload['externalIdFieldName'] = external_id_field

        url = self.bulk_url

        result = call_salesforce(url=url, method='POST', session=self.session,
                                  headers=self.headers,
                                  data=json.dumps(payload))
        return result.json(object_pairs_hook=OrderedDict)

    def _add_batch(self, job_id, data, operation):
        """ Add a set of data as a batch to an existing job
        Separating this out in case of later
        implementations involving multiple batches
        """

        url = "{}{}{}".format(self.bulk_url, job_id, '/batches/')

        headers = dict(self.headers, **{'Content-Type': 'text/csv'})

        result = call_salesforce(url=url, method='PUT', session=self.session,
                                  headers=headers, data=data)
        return result.status_code

    def _close_job(self, job_id):
        """ Close a bulk job """
        payload = {
            'state': 'UploadComplete'
        }

        url = "{}{}".format(self.bulk_url, job_id)

        result = call_salesforce(url=url, method='PATCH', session=self.session,
                                  headers=self.headers,
                                  data=json.dumps(payload))
        return result.json(object_pairs_hook=OrderedDict)

    def _get_job(self, job_id):
        """ Get an existing job to check the status """
        url = "{}{}".format(self.bulk_url, job_id)

        result = call_salesforce(url=url, method='GET', session=self.session,
                                  headers=self.headers)
        return result.json(object_pairs_hook=OrderedDict)

    def _get_job_results(self, job_id):
        """ retrieve a set of results from a completed job """

        url = "{}{}{}".format(self.bulk_url, job_id, '/successfulResults/')

        result_success = call_salesforce(url=url, method='GET',
                            session=self.session, headers=self.headers).content.decode('utf-8')

        url = "{}{}{}".format(self.bulk_url, job_id, '/failedResults/')

        result_failed = call_salesforce(url=url, method='GET', 
                            session=self.session, headers=self.headers).content.decode('utf-8')

        url = "{}{}{}".format(self.bulk_url, job_id, '/unprocessedrecords/')

        result_unprocessed = call_salesforce(url=url, method='GET', 
                            session=self.session, headers=self.headers).content.decode('utf-8')

        combined_result = result_success.splitlines()[0]

        if len(result_success.splitlines()) > 1:
            combined_result += '\n' + '\n'.join(result_success.splitlines()[1:])
        
        if len(result_failed.splitlines()) > 1:
            combined_result += '\n' + '\n'.join(result_failed.splitlines()[1:])

        if len(result_unprocessed.splitlines()) > 1:
            empty_columns = '\n"","",'
            combined_result += empty_columns + empty_columns.join(result_unprocessed.splitlines()[1:])

        return combined_result

    #pylint: disable=R0913
    def _bulk_operation(self, object_name, operation, data,
                        external_id_field=None, wait=5):
        """ String together helper functions to create a complete
        end-to-end bulk v2 API request

        Arguments:

        * object_name -- SF object
        * operation -- Bulk operation to be performed by job
        * data -- csvreader or list of dict to be passed as a batch
        * external_id_field -- unique identifier field for upsert operations
        * wait -- seconds to sleep between checking batch status
        """
        
        #if we are provided a list of json objects, turn it into a file object
        if type(data)==list:
            file = json_list_to_file(data)
            data = csv.reader(file, delimiter=',', quotechar='"')
        
        job_ids = []
        for batch in split_csv(data, 100):
            with open('batch.csv', 'w') as f:
                f.write(batch)

            job = self._create_job(object_name=object_name, operation=operation,
                                external_id_field=external_id_field)
            
            self._add_batch(job_id=job['id'], data=batch, operation=operation)

            self._close_job(job_id=job['id'])
            
            job_ids.append(job['id'])
            
        results = []
        while job_ids:
            for i, job in enumerate(job_ids):
                job_state = self._get_job(job_id=job)['state']
                
                if job_state not in ['JobComplete', 'Failed', 'Aborted']:
                    continue

                #todo: handle failed job states, e.g. ClientInputError : LineEnding is invalid on user data. Current LineEnding setting is LF
                job_ids.pop(i)
                job_result = self._get_job_results(job_id=job)
                results.append(job_result)
            sleep(wait)
        
        return "\n".join(results)

    # _bulk_operation wrappers to expose supported Salesforce bulk operations
    def delete(self, data):
        """ soft delete records """
        results = self._bulk_operation(object_name=self.object_name,
                                       operation='delete', data=data)
        return results

    def insert(self, data):
        """ insert/create records """
        results = self._bulk_operation(object_name=self.object_name,
                                       operation='insert', data=data)
        return results

    def upsert(self, data, external_id_field):
        """ upsert records based on a unique identifier """
        results = self._bulk_operation(object_name=self.object_name,
                                       operation='upsert',
                                       external_id_field=external_id_field,
                                       data=data)
        return results

    def update(self, data):
        """ update records """
        results = self._bulk_operation(object_name=self.object_name,
                                       operation='update', data=data)
        return results

# def seeksplit_unsafe(data, split_size=100):
#     """Returns a list of CSVs that are under the split_size

#         Arguments:

#         * data -- CSV file of data
#         * split_size -- maximum size of each chunk, in MB. Bulk API v2 cannot exceed 100MB in utf-8 encoding"""
#     #todo: newline handling? \r\n to \n ?
#     max_bytes = int(split_size * 1024 * 1024)
#     headers = data.readline()
#     content = data.read(max_bytes) + data.readline()
#     # i = 1
#     while content != '':
#         # with open("upload{}.csv".format(i), "w") as f:
#         #     f.write(headers + content)
#         yield (headers + content).replace('\n', os.linesep)
#         content = data.read(max_bytes) + data.readline()
#         # i = i+1
        
# def split_file_unsafe(data, split_size=100):
#     """Returns a list of CSVs that are under the split_size

#         Arguments:

#         * data -- CSV file of data
#         * split_size -- maximum size of each chunk, in MB. Bulk API v2 cannot exceed 100MB in utf-8 encoding"""
#     #todo: newline handling? \r\n to \n ?
#     max_bytes = int(split_size * 1024 * 1024)
#     header = next(data)
#     for line in data:
#         output = io.StringIO(newline=None)
#         output.write(header)
#         n = 0
#         for line in chain([line], data):
#             output.write(line)
#             n += len(line)
#             if n >= max_bytes:
#                 yield output
#                 break
#     yield output

