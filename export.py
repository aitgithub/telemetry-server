#!/usr/bin/env python
"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""

import sys, os
import boto
from boto.s3.key import Key
from datetime import datetime

usage = """Usage: python export.py s3_bucket file1 [ file2 ] [ file3 ] [ ... ]
   Put your AWS credentials into ~/.boto
"""


class Exporter:
    """A class for exporting Telemetry data to Amazon S3"""

    def __init__(self, bucket):
        print "Using bucket name", bucket
        self.conn = boto.connect_s3()
        try:
            self.bucket = self.conn.get_bucket(bucket)
        except boto.exception.S3ResponseError, e:
            if e.status == 404:
                # TODO: specify location=X and policy='private'?
                self.bucket = self.conn.create_bucket(bucket)

    def progress(self, bytes_sent, total_bytes):
        print "Sent", bytes_sent, "of", total_bytes

    def filename_to_dims(self, filename):
        # Convert the specified to a key

        # TODO: get this # of pieces from TelemetrySchema
        pieces = 5
        dims = [''] * pieces
        # TODO: what about files without the correct number of dimensions?
        base = os.path.normpath(filename)
        for i in range(pieces):
            base, end = os.path.split(base)
            dims[pieces-i-1] = end
        #print "Converted", filename, "to", dims
        return dims

    def dims_to_key(self, dims):
        return os.path.join(*dims)
    
    def export(self, filename):
        print "Exporting", filename
        start = datetime.now()
        k = Key(self.bucket)
        # TODO: k.set_canned_acl("private")?

        dims = self.filename_to_dims(filename)
        if '' in dims:
            print "WARNING:", filename, "was missing some dimensions"
        k.key = self.dims_to_key(dims)

        # TODO: check if it exists already, and whether it's the same.
        k.set_contents_from_filename(filename, cb=self.progress, num_cb=100)
        delta = (datetime.now() - start)
        mb_sent = float(os.path.getsize(filename)) / 1024.0 / 1024.0
        seconds = float(delta.seconds) + float(delta.microseconds) / 1000000
        rate = 0.0
        if seconds > 0.0:
            rate = mb_sent / seconds
        print "Sent %s with key '%s': %.2fMB in %.1f (%.2fMB/s)" % (filename, k.key, mb_sent, seconds, rate)
        # TODO: set_metadata(dimension, dimension_value) for each dim

def main(argv=None):
    if argv is None:
        argv = sys.argv

    if len(argv) < 3:
        print usage
        return 2

    bucket = argv[1]
    exporter = Exporter(bucket)
    for f in argv[2:]:
        exporter.export(f)

if __name__ == "__main__":
    sys.exit(main())
