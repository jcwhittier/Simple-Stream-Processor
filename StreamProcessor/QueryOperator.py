__author__ = 'jcwhittier'

from enum import Enum
from CountWindow import CountWindow
from abc import abstractmethod, ABC


class QueryOperator(ABC):

    def __init__(self, upstream_operator=None):
        if upstream_operator is None or isinstance(upstream_operator, QueryOperator):
            self.upstream_operator = upstream_operator
        else:
            raise TypeError("Given query_operator is not a QueryOperator")

    # @property
    # def upstream_operator(self):
    #     return self.__upstream_operator
    #
    # @upstream_operator.setter
    # def upstream_operator(self, upstream_op):
    #     if upstream_op is None or isinstance(upstream_op, QueryOperator):
    #         self.upstream_operator = upstream_op
    #     else:
    #         raise TypeError("Given query_operator is not a QueryOperator")

    def get_most_upstream(self):
        """Recursively get the most upstream operator"""
        upstream = self.upstream_operator
        if upstream is None:
            return self
        else:
            return upstream.get_most_upstream()

    def process_tuple(self, tup):
        if self.upstream_operator:
            if isinstance(self.upstream_operator, QueryOperator):
                query_operator_input = self.upstream_operator.process_tuple(tup)
            else:
                raise TypeError("Given query_operator is not a QueryOperator")
        else:
            query_operator_input = tup

        return self.evaluate_operator(query_operator_input)

    @abstractmethod
    def evaluate_operator(self, tup):
        raise NotImplementedError



class Filter(QueryOperator, ABC):
    """A generic filter for both pass and drop filters"""

    def __init__(self, column_name, threshold, comparator, query_operator=None):
        super().__init__(query_operator)
        self.threshold = threshold
        self.column_name = column_name
        self.comparator = comparator

    class FilterType(Enum):
        greater_than = (lambda x, y: x > y)
        greater_than_or_equal = (lambda x, y: x >= y)
        less_than = (lambda x, y: x < y)
        less_than_or_equal = (lambda x, y: x <= y)
        equal = (lambda x, y: x == y)

    def do_compare(self, tup):

        if tup and tup.get(self.column_name, None) and \
                (self.comparator(float(tup.get(self.column_name, None)), float(self.threshold))):
            return True
        else:
            return False


class PassFilter(Filter):
    """A generic filter to pass data that satisfies a given comparator"""

    # def __init__(self, column_name, threshold, comparator):
    #     super().__init__(column_name, threshold, comparator)

    def __init__(self, column_name, threshold, comparator, query_operator=None):
        super().__init__(column_name, threshold, comparator, query_operator)

    def evaluate_operator(self, tup):
        """if the comparison is True, pass tup and output tup, otherwise return None"""
        if self.do_compare(tup):
            return tup
        else:
            return None


class SpatialBBFilterInside(QueryOperator):
    """Return true if a point if lon,lat is inside the given bounds.
    Completing this Class is left as a homework exercise"""

    def __init__(self, lat_column_name, lon_column_name, xmin, xmax, ymin, ymax, query_operator=None):
        super().__init__(query_operator)
        self.lon_column_name = lon_column_name
        self.lat_column_name = lat_column_name
        self.xmin = xmin
        self.xmax = xmax
        self.ymin = ymin
        self.ymax = ymax

    def do_compare(self, tup):
        """Completing this function is left as a homework exercise"""
        if tup and tup.get(self.lon_column_name, None) and \
                tup.get(self.lon_column_name, None) > self.xmin:
            return True
        else:
            return False

    def evaluate_operator(self, tup):
        """if the comparison is True, pass tup and output tup, otherwise return None."""
        if self.do_compare(tup):
            return tup
        else:
            return None


class DropFilter(Filter):
    """A generic filter to drop data that satisfies a given comparator"""

    def __init__(self, column_name, threshold, comparator, query_operator=None):
        super().__init__(column_name, threshold, comparator, query_operator)

    def evaluate_operator(self, tup):
        """if the comparison is True, drop tup and output none instead otherwise return tup"""
        if self.do_compare(tup):
            return None
        else:
            return tup


class BatchOperator(ABC):
    """An empty class defining the methods to be implemented in the inherited class implementations"""

    def __init__(self, column_name, output_column_name):
        print("BatchOperator init called with column_name", column_name)
        # super().__init__(self)
        self.column_name = column_name
        self.output_column_name = output_column_name

    @abstractmethod
    def get_result(self):
        raise NotImplementedError

    @abstractmethod
    def clear(self):
        raise NotImplementedError


class BatchMeanOperator(BatchOperator):
    """An operator for calculating the mean"""

    def __init__(self, column_name, output_column_name):
        print("calling super init with column_name", column_name)
        super().__init__(column_name, output_column_name)
        self.sum = 0
        self.tuple_count = 0

    def evaluate_operator(self, tup):
        if tup and tup.get(self.column_name, None):
            self.sum += tup.get(self.column_name, None)
            self.tuple_count += 1

    def get_result(self):
        if self.tuple_count > 0:
            return {self.output_column_name: self.sum / self.tuple_count}
        else:
            return None

    def clear(self):
        self.sum = 0
        self.tuple_count = 0



class BatchMaxOperator(BatchOperator):
    """An operator for calculating the max"""
    def __init__(self, column_name, output_column_name):
        super().__init__(column_name, output_column_name)
        self.max = None

    def evaluate_operator(self, tup):
        if tup and tup.get(self.column_name, None) and \
                (self.max is None or tup.get(self.column_name, None) > self.max):
            self.max = tup.get(self.column_name, None)

    def get_result(self):
        return {self.output_column_name: self.max}

    def clear(self):
        self.max = None


class BatchMinOperator(BatchOperator):
    """An operator for calculating the min"""
    def __init__(self, column_name, output_column_name):
        super().__init__(column_name, output_column_name)
        self.min = None

    def evaluate_operator(self, tup):
        if tup and tup.get(self.column_name, None) and \
                (self.min is None or tup.get(self.column_name, None) < self.min):
            self.min = tup.get(self.column_name, None)

    def get_result(self):
        return {self.output_column_name: self.min}

    def clear(self):
        self.min = None


class PipelinedAggregate(QueryOperator):

    def __init__(self, window_size, window_slide, operator, query_operator=None):
        super().__init__(query_operator)
        self.window_size = window_size
        self.window_slide = window_slide
        self.window = CountWindow(window_size)
        self.operator = operator
        self.tuple_count = 0

    def evaluate_operator(self, tup):

        if tup:

            self.tuple_count += 1

            self.window.add_tuple(tup)

            self.operator.clear()

            for t in self.window.get_tuples():
                self.operator.evaluate_operator(t)

            if self.is_end_of_window():
                self.tuple_count = 0
                return self.operator.get_result()

        return None

    def is_end_of_window(self):
        if self.window.count_tuples_in_window() == self.window_size \
                and self.tuple_count >= self.window_slide:
            return True
        else:
            return False

