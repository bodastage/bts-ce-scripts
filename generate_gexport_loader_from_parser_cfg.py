# Generate gexport_loader_from_parser_cfg
#
# gexport_loader_from_parser_cfg.py huawei_gexport_gsm BSC6900GSM /mediation/conf/cm/gexport_gsm.cfg
#
# Example: python gexport_loader_from_parser_cfg.py huawei_gexport_gsm "data/cm/conf/gexport_gsm.cfg"

# Licence: Apache 2.0
#
import os
import sys
import csv


if len(sys.argv) != 4:
    print("Format: {0} {1} {2} {3}".format(os.path.basename(__file__), "<schema>", "<netype>", "<parser config file>"))
    sys.exit()

schema = sys.argv[1]
ne_type = sys.argv[2]
parser_cfg = sys.argv[3]
csv_dir='/mediation/data/cm/huawei/parsed/gexport_gsm/'


print( "-- psql -U bodastage -d database -a -f {}.load.sql".format(schema) )

with open(parser_cfg) as f:
    data = f.readlines()

    for row in data:
        mo_and_params = row.rstrip().split(":")
        mo = mo_and_params[0]

        file_path = "{}/{}_{}.csv".format(csv_dir, mo, ne_type)

        # truncate table
        print("TRUNCATE TABLE {}.\"{}_{}\";".format(schema, mo, ne_type))
        print("\COPY {}.\"{}_{}\" FROM '{}' CSV HEADER;".format(schema, mo, ne_type, file_path))
        print("")



