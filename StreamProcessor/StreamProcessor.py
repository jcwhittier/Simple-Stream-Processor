__author__ = 'jcwhittier'

import argparse
import serial
import fileinput
from StreamAdapter import *
from QueryOperator import *
from Query import *

if sys.version_info < (3, 4):
    raise SystemExit("This script requires Python 3.4 or later. You are running: " + sys.version_info)


class InputSource(Enum):
    """An enum to represent the different input options for the program"""
    ARDUINO = 4
    FILE = 5
    STDIN = 6


def configure(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--filename',
                        required=False,
                        type=str,
                        default=None,
                        dest="filename",
                        metavar="<file to process>",
                        help="Name of file to process")

    parser.add_argument('-b', '--baud_rate',
                        required=False,
                        type=int,
                        default=9600,
                        dest="baud_rate",
                        metavar="<baud rate of Arduino>",
                        help="The baud baud rate of Arduino")

    parser.add_argument('-s', '--stdin',
                        required=False,
                        action='store_true',
                        default=None,
                        dest="stdin",
                        # metavar="<Read from Standard Input (STDIN)?>",
                        help="Read from Standard Input (STDIN)?")

    parser.add_argument('-p', '--port',
                        required=False,
                        type=str,
                        default=None,
                        dest="serial_port",
                        metavar="<serial port for Arduino>",
                        help="""Specify the address of your serial port. You can find this in the Arduino IDE by going
                        to Tools>Port and finding the name of the serial port for your Arduino.
                        On Windows your port may have a name similar to COM3 and on MacOS is may look like
                        /dev/cu.usbmodemFD13131""")

    args = parser.parse_args(argv)

    filename, serial_port, baud_rate = args.filename, args.serial_port, args.baud_rate

    if filename and not args.stdin and not serial_port:
        input_source = InputSource.FILE
    elif not filename and args.stdin and not serial_port:
        input_source = InputSource.STDIN
        filename = '-'  # '-' tells fileinput read from stdin
    elif not filename and not args.stdin and serial_port:
        input_source = InputSource.ARDUINO
    else:
        parser.print_usage()
        sys.exit(1)

    return input_source, filename, serial_port, baud_rate


def run(input_source, filename, serial_port, baud_rate):
    if input_source in [InputSource.FILE, InputSource.STDIN]:

        # Query Window Specification
        window_length = 10
        window_slide = 2

        mean_hr_query = Query(
            PipelinedAggregate(window_length, window_slide, BatchMeanOperator("heart_rate", "mean_heart_rate")))

        mean_hr_range_170_190 = Query(
            PipelinedAggregate(window_length, window_slide, BatchMeanOperator("heart_rate", "mean_heart_rate")),
            PassFilter("mean_heart_rate", 190, Filter.FilterType.less_than),
            PassFilter("mean_heart_rate", 170, Filter.FilterType.greater_than_or_equal)
        )

        min_hr_query = Query(
            PipelinedAggregate(window_length, window_slide, BatchMinOperator("heart_rate", "min_heart_rate")))

        max_hr_query = Query(
            PipelinedAggregate(window_length, window_slide, BatchMaxOperator("heart_rate", "max_heart_rate")))

        # Filters for the bounding box: -68.66751, 44.90601 ; -68.66072, 44.90464
        high_pass_lon = PassFilter("lon", -68.66751, Filter.FilterType.greater_than)
        low_pass_lon = PassFilter("lon", -68.66072, Filter.FilterType.less_than)
        high_pass_lat = PassFilter("lat", 44.90464, Filter.FilterType.greater_than)
        low_pass_lat = PassFilter("lat", 44.90601, Filter.FilterType.less_than)

        bounding_box_filters = Query(high_pass_lon, low_pass_lon, high_pass_lat, low_pass_lat)

        # the list of queries to iterate through for the input file
        queries_to_run = [mean_hr_query,
                          mean_hr_range_170_190, min_hr_query, max_hr_query, bounding_box_filters]

        class MyStreamInputAdapter(StreamInputAdapter):

            def __init__(self, input_stream, loop_delay):
                super(MyStreamInputAdapter, self).__init__(input_stream, loop_delay)

            @staticmethod
            def transform_stream_tuple(tup):
                tup = tup[:-1]  # remove the /n from the stream tuple

                id_index, lon_index, lat_index, tick_index, hr_index = 0, 2, 3, 4, 5

                tup = tup.split(",")

                transformed_tup = {"runner_id": int(tup[id_index]), "lon": float(tup[lon_index]),
                                   "lat": float(tup[lat_index]), "tick": int(tup[tick_index]),
                                   "heart_rate": int(tup[hr_index])}

                return transformed_tup

        for query in queries_to_run:
            with fileinput.input(filename) as f:
                loop_delay = 0.0001
                stream_input_adapter = MyStreamInputAdapter(f, loop_delay)
                stream_input_adapter.run(query)

    elif input_source is InputSource.ARDUINO:

        class MyArduinoAdapter(ArduinoAdapter):

            @staticmethod
            def transform_stream_tuple(tup):
                tup = tup[:-2]  # remove the /r/n from the tuple
                tup = tup.split(",")
                tup[0] = int(tup[0])
                tup[1] = int(tup[1])
                transformed_tup = {"id": tup[0], "value": tup[1]}
                return transformed_tup

        ser = serial.Serial(port=serial_port, baudrate=baud_rate, timeout=None)
        arduino_adapter = MyArduinoAdapter(ser)

        # Query Setup
        threshold = 10
        window_length = 10
        window_slide = 5

        # a high pass filter created using a lambda function for greater than
        high_pass_lambda = PassFilter("id", threshold, lambda x, y: x > y)

        # a high pass filter created using the lambda greater than function defined in Filter.FilterType.greater_than
        high_pass_enum = PassFilter("id", threshold, Filter.FilterType.greater_than)

        mean_aggregate = PipelinedAggregate(window_length, window_slide, BatchMeanOperator("id"))

        arduino_adapter.run(Query(high_pass_lambda))
        # arduino_adapter.run(Query(high_pass_enum))
        # arduino_adapter.run(Query(mean_aggregate))

    else:
        print("Input of " + str(input_source) + " is not supported")


def main(argv=None):
    input_source, filename, serial_port, baud_rate = configure(argv)
    run(input_source, filename, serial_port, baud_rate)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
