# Generate gexport alembic migration script from parser config
#
# generate_gexport_alembic_migrations_from_parser_cfg.py huawei_gexport_gsm BSC6900GSM /mediation/conf/cm/gexport_gsm.cfg
#
# Example: python generate_gexport_alembic_migrations_from_parser_cfg.py huawei_gexport_gsm "data/cm/conf/gexport_gsm.cfg"

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

mo_list = []

with open(parser_cfg) as f:
    data = f.readlines()


    for row in data:
        mo_and_params = row.rstrip().split(":")
        mo = mo_and_params[0]
        mo_list.append(mo)

        parameters = mo_and_params[1]
        param_list = parameters.split(",")
        print("op.create_table('{}_{}',".format(mo, ne_type))
        for param in param_list:
            if param == 'DATETIME' or param == 'varDateTime':
                print("    sa.Column('{}', sa.DateTime, autoincrement=False, nullable=True),".format(param))
            else:
                print("    sa.Column('{}', sa.CHAR(length=250), autoincrement=False, nullable=True),".format(param))
        print("schema='{}'".format(schema))
        print(")")
        print("")



for mo in mo_list:
    print("op.drop_table('{}_{}', schema='{}')".format(mo, ne_type, schema))
