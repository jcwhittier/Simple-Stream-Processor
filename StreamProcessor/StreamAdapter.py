__author__ = 'jcwhittier'

import time
import io
from abc import ABC, abstractmethod
from fileinput import FileInput


class StreamInputAdapter(ABC):

    @abstractmethod
    def __init__(self, input_stream, loop_delay=0):
        if self.valid_input_stream(input_stream):
            self.input_stream = input_stream
            self.loop_delay = loop_delay
        else:
            raise TypeError("can't convert input_stream to io.IOBase")

    def run(self, query):
        if not self.valid_input_stream(self.input_stream):
            raise TypeError("can't convert input_stream to io.IOBase or FileInput")
        for line in self.input_stream:
            query.process_tuple(self.transform_stream_tuple(line))
            time.sleep(self.loop_delay)

    @staticmethod
    def valid_input_stream(input_stream):
        return isinstance(input_stream, io.IOBase) or isinstance(input_stream, FileInput)

    @staticmethod
    @abstractmethod
    def transform_stream_tuple(tup):
        raise NotImplementedError

class ArduinoAdapter(StreamInputAdapter, ABC):

    def __init__(self, serial_port):
        self.serial_port = serial_port
        self.serial_port.flushInput()
        time.sleep(2)

    def run(self, query):
        while True:
            tup = self.serial_port.readline().decode("ascii")
            query.process_tuple(self.transform_stream_tuple(tup))
