#!/usr/bin/env python
# -*- coding: UTF-8 -*-

"""Python module to query data available in AWS Athena.

Can also use as a command line utility:

    $ query="SELECT * FROM tbl;"
    $ python athena.py "$query" > results.tsv

To include header in output:

    # python athena.py "$query" --header > results.tsv

TODO
----
- athena.query() needs error handling/checking during polling!

"""

import argparse
import csv
import sys
import time

import boto3

class Athena(object):
    """ Interface to AWS Athena, establishing settings, and allows querying."""

    def __init__(self, database='default', s3_path='s3://cmathenalogs/'):
        self.client = boto3.client('athena')
        self.database = database
        self.s3_path = s3_path

    def query(self, query):
        """ Starts specified query. Returns AthenaResult object. After query
        starts, polls AWS until the query is finished."""
        response = self.client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': self.database},
            ResultConfiguration={'OutputLocation': self.s3_path})
        query_id = response['QueryExecutionId']
        time.sleep(3)
        while True:
            try:
                self.client.get_query_results(QueryExecutionId=query_id)
                break
            except Exception:
                time.sleep(10)
        return AthenaResult(self.client, self.database, query_id, self.s3_path)

    def repair_table(self, table):
        """ Repairs Athena table."""
        query = 'MSCK REPAIR TABLE %s' % table
        self.query(query)


class AthenaResult(object):
    """ Contains metadata and results of an Athena query. Includes methods to
    format and output the data."""

    def __init__(self, client, database, query_id, s3_path):
        self.client = client
        self.paginator = self.client.get_paginator('get_query_results')
        self.database = database
        self.query_id = query_id
        self.s3_path = self._result_csv(s3_path, query_id)
        self.results = None
        self.results_str = None

    def result(self, header=True):
        """ Returns result of query."""
        query_results = self.client.get_query_results(QueryExecutionId=self.query_id)
        page_iterator = self.paginator.paginate(QueryExecutionId=self.query_id)
        results = []
        for page in page_iterator:
            results.append(page)

        if len(results) == 0:
            return []

        result_sets = [i['ResultSet']['Rows'] for i in results]
        rows = [y for x in result_sets for y in x]
        data = [row['Data'] for row in rows]

        final_data = []
        for d in data:
            datum = []
            for e in d:
                datum.append(e.get('VarCharValue', u''))
            final_data.append(datum)
        self.results = final_data
        if len(final_data) > 0:
            final_data.pop(0)

        return final_data

    def to_string(self, delim='\t'):
        """ Converts result to delimited string."""
        if self.results == None:
            self.results = self.result()
        lines = [delim.join(i) for i in self.results]
        as_string = '\n'.join(lines) + '\n'
        self.results_str = as_string
        return as_string

    def to_s3(self, bucket, file_name):
        """ Save computed result to S3 file."""
        if self.results_str == None:
            self.results_str = self.to_string()
        s3 = boto3.resource('s3')
        obj = s3.Object(bucket, file_name)
        obj.put(Body=self.results_str)

    def _format_results(self, query_results, header=True):
        """ Converts Athena results into list-tuple format."""
        rows = query_results['ResultSet']['Rows']
        matrix = []
        for r in rows:
            row = r['Data']
            datum = []
            for j in row:
                try:
                    val = j['VarCharValue']
                except KeyError:
                    continue
                datum.append(val)
            matrix.append(tuple(datum))
        if header == False:
            matrix.pop(0)
        return matrix

    def _result_csv(self, s3_path, query_id):
        """ Returns filepath of Athena query results stored in S3."""
        if s3_path.endswith('/') == False:
            s3_path += '/'
        return s3_path + query_id + '.csv'


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('query', type=str,
                        help='Query for Athena to run')
    parser.add_argument('--header', action='store_true',
                        help='Output includes header')
    args = parser.parse_args()
    q = args.query
    h = args.header

    athena = Athena()
    r = athena.query(q)

    stoud_writer = csv.writer(sys.stdout, delimiter='\t',
                              escapechar='\\', quoting=csv.QUOTE_NONE,
                              lineterminator='\n')
    for row in r.result(header=h):
        stoud_writer.writerow(row)
