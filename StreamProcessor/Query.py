__author__ = 'jcwhittier'

import sys
from enum import Enum


class Query(object):
    class QueryOutputFormat(Enum):
        STRING = 1
        STREAM_TUPLE = 2

    def __init__(self, *query_pipeline):
        self.query_pipeline = None
        for query_op in query_pipeline:
            self.append_downstream(query_op)
        self.output_file = sys.stdout
        self.query_output_format = self.QueryOutputFormat.STREAM_TUPLE

    def clear(self):
        self.query_pipeline = None

    def append_upstream(self, query_to_append):
        if not self.query_pipeline:
            self.query_pipeline = self.operator_in_query(query_to_append)
        else:
            self.query_pipeline.get_most_upstream().upstream_operator = self.operator_in_query(query_to_append)

    def append_downstream(self, query_to_append):
        new_query = self.operator_in_query(query_to_append)

        if self.query_pipeline:
            new_query.get_most_upstream().upstream_operator = self.query_pipeline

        self.query_pipeline = new_query

    def process_tuple(self, tup):
        result = None
        if self.query_pipeline:
            result = self.query_pipeline.process_tuple(tup)
        if result:
            if self.query_output_format is self.QueryOutputFormat.STRING:
                result = str(result)
            print(result, file=self.output_file)

    @staticmethod
    def operator_in_query(query):
        if query and isinstance(query, Query):
            return query.query_pipeline
        return query
