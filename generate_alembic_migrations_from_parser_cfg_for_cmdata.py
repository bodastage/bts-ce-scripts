# Generate gexport alembic migration script from parser config for final cm data
#
# This adds the load id to the tables
#
# generate_alembic_migrations_from_parser_cfg.py huawei_gexport_gsm /mediation/conf/cm/gexport_gsm.cfg
#
# Example: python generate_alembic_migrations_from_parser_cfg.py huawei_gexport_gsm "data/cm/conf/gexport_gsm.cfg"

# Licence: Apache 2.0
#

import os
import sys
import csv
import logging

if len(sys.argv) != 3:
    print("Format: {0} {1} {2}".format(os.path.basename(__file__), "<schema>", "<parser config file>"))
    sys.exit()

schema = sys.argv[1]
parser_cfg = None

parser_cfg = sys.argv[2]


mo_list = []

text_fields = ["LAI","CAPACITYLOCKS","UMFI_IDLE","ULPRIOTHR","UQRXLEVMIN","COVERAGEU","UHPRIOTHR", "reservedBy",
                "certificateContent_publicKey", "sectorCarrierRef", "trustedCertificates", "consistsOf","consistsOf","listOfNe",
                "equipmentClockPriorityTable","rfBranchRef", "syncRiPortCandidate", "transceiverRef","vsdatamulticastantennabranch",
                "associatedRadioNodes","ethernetPortRef","staticRoutes_ipAddress", "staticRoutes_networkMask",
                "staticRoutes_nextHopIpAddr","staticRoutes_redistribute","ipInterfaceMoRef","ipAccessHostRef",
                "physicalPortList","additionalText","candNeighborRel_enbId", "vlanRef","trDeviceRef","port","iubLinkUtranCell",
                "manages","iubLinkUtranCell", "FREQLST"]

print("def upgrade():")
with open(parser_cfg) as f:
    data = f.readlines()


    for row in data:
        mo_and_params = row.rstrip().split(":")
        mo = mo_and_params[0]
        mo_list.append(mo)

        parameters = mo_and_params[1]
        param_list = parameters.split(",")
        print("    op.create_table('{}',".format(mo))



        # Add load id. This is the pk of the cm_loads table
        print("        sa.Column('LOADID', sa.Integer, autoincrement=False, nullable=False),")

        for param in param_list:

            # Remove VENDOR fields because it makes no sence
            # if schema == 'huawei_cm_2g':
            #     if param in ['VENDOR']: continue

            if param == 'DATETIME' or param == 'varDateTime':
                print("        sa.Column('{}', sa.DateTime, autoincrement=False, nullable=True),".format(param))
            elif param in text_fields or param[-3:].lower() == 'ref':
                print("        sa.Column('{}', sa.Text, autoincrement=False, nullable=True),".format(param))
            else:
                print("        sa.Column('{}', sa.CHAR(length=250), autoincrement=False, nullable=True),".format(param))
        print("        schema='{}'".format(schema))
        print("    )")
        print("")


print("def downgrade():")
for mo in mo_list:
    print("    op.drop_table('{}', schema='{}')".format(mo, schema))
