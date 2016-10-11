import pytz
import dateutil.parser


def parse_timestamp(txt):
    try:
        dt = dateutil.parser.parse(txt)
    except:
        print(txt)
        raise
    if dt.tzinfo is None:
        # Assume UTC
        dt = dt.replace(tzinfo=pytz.utc)
    return dt


def format_date(timestamp):
    offset = timestamp.utcoffset()
    if (offset is None) or (timestamp.utcoffset().total_seconds() != 0):
        raise ValueError("timestamp must be in UTC")
    return timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")


def write_ranges(ranges, path):
    with open(path, "w") as f:
        f.write("mmsi,start_time,end_time,is_fishing\n")
        for row in ranges:
            mmsi, start, stop, is_fishing = row
            start = format_date(start)
            stop = format_date(stop)
            f.write("{}\n".format(','.join(
                str(x) for x in [mmsi, start, stop, is_fishing])))


def write_recarray(recarry, path):
    with open(path, "w") as f:
        f.write(','.join(str(x) for x in recarry.dtype.names) + '\n')
        for row in recarry:
            f.write(','.join(str(x) for x in row) + '\n')
