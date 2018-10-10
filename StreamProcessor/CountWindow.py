__author__ = 'jcwhittier'


class CountWindow(object):
    """Constructor to create a new count-based window of a specified size"""
    def __init__(self, window_size):
        self.window_size = window_size
        self.window_tuples = []

    def add_tuple(self, tuple):
        """Add a new tuple to the window and make sure that the oldest tuple is removed if the window is full"""
        self.window_tuples.append(tuple)
        if self.count_tuples_in_window() > self.window_size:
            self.window_tuples.pop(0)


    def get_tuples(self):
        """Get all the tuples currently in the window"""
        return self.window_tuples

    def clear_tuples(self):
        """Clear the tuples in the window"""
        self.window_tuples = []

    def count_tuples_in_window(self):
        """Get the number of tuples in the window"""
        return len(self.window_tuples)
