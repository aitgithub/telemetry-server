import sys, struct, re, os, argparse, gzip, StringIO
import simplejson as json
from persist import StorageLayout
from telemetry_schema import TelemetrySchema
from datetime import date, datetime
import util.timer as timer

filename_timestamp_pattern = re.compile("^telemetry.log.([0-9]+).([0-9]+)(.finished)?$")

def main():
    parser = argparse.ArgumentParser(description='Split raw logs into partitioned files.', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-m", "--max-output-size", metavar="N", help="Rotate output files after N bytes", type=int, default=500000000)
    parser.add_argument("-i", "--input-file", help="Filename to read from", required=True)
    parser.add_argument("-o", "--output-dir", help="Base directory to store split files", required=True)
    parser.add_argument("-t", "--telemetry-schema", help="Filename of telemetry schema spec", required=True)
    parser.add_argument("-b", "--bucket", help="S3 Bucket name")
    parser.add_argument("-k", "--aws-key", help="AWS Key")
    parser.add_argument("-s", "--aws-secret-key", help="AWS Secret Key")
    args = parser.parse_args()

    schema_data = open(args.telemetry_schema)
    schema = TelemetrySchema(json.load(schema_data))
    schema_data.close()

    storage = StorageLayout(schema, args.output_dir, args.max_output_size)

    expected_dim_count = len(schema._dimensions)

    if not os.path.isdir(args.output_dir):
        os.makedirs(args.output_dir)

    record_count = 0;
    fin = open(args.input_file, "rb")

    bytes_read = 0
    start = datetime.now()
    while True:
        record_count += 1
        # Read two 4-byte values and one 8-byte value
        lengths = fin.read(16)
        if lengths == '':
            break
        len_path, len_data, timestamp = struct.unpack("<IIQ", lengths)

        # Incoming timestamps are in milliseconds, so convert to POSIX first
        # (ie. seconds)
        submission_date = date.fromtimestamp(timestamp / 1000).strftime("%Y%m%d")
        path = unicode(fin.read(len_path), errors="replace")
        #print "Path for record", record_count, path, "length of data:", len_data

        # Detect and handle gzipped data.
        data = fin.read(len_data)
        try:
            # Note: from brief testing, cStringIO doesn't appear to be any
            #       faster. In fact, it seems slightly slower than StringIO.
            data_reader = StringIO.StringIO(data)
            uncompressor = gzip.GzipFile(fileobj=data_reader, mode="r")
            data = unicode(uncompressor.read(), errors="replace")
            uncompressor.close()
            data_reader.close()
        except Exception, e:
            #print e
            # Use the string as-is
            data = unicode(data, errors="replace")

        bytes_read += 8 + len_path + len_data
        #print "Path for record", record_count, path, "length of data:", len_data, "data:", data[0:5] + "..."

        path_components = path.split("/")
        if len(path_components) != expected_dim_count:
            # We're going to pop the ID off, but we'll also add the submission,
            # so it evens out.
            print "Found an invalid path in record", record_count, path
            continue

        key = path_components.pop(0)
        info = {}
        info["reason"] = path_components.pop(0)
        info["appName"] = path_components.pop(0)
        info["appVersion"] = path_components.pop(0)
        info["appUpdateChannel"] = path_components.pop(0)
        info["appBuildID"] = path_components.pop(0)
        dimensions = schema.dimensions_from(info, submission_date)
        #print "  Converted path to filename", schema.get_filename(args.output_dir, dimensions)
        storage.write(key, data, dimensions)
    duration = timer.delta_sec(start)
    mb_read = bytes_read / 1024.0 / 1024.0
    print "Read %.2fMB in %.2fs (%.2fMB/s)" % (mb_read, duration, mb_read / duration)
    return 0

if __name__ == "__main__":
    sys.exit(main())



