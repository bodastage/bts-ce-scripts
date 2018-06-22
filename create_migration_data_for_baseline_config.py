from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
import time
import timeit
import sys
import os
import logging

# Populate the normalized table using insert tables
#
# ZTE 2G
# insert into normalized_managedobjects
# (name, vendor_pk, tech_pk, modified_by, added_by)
# SELECT
# table_name as name ,
# 3 as vendor_pk,
# 1 as tech_pk,
# 0 as modified_by,
# 0 as added_by
# FROM information_schema."views"
# where
# table_schema  = 'zte_cm_2g'

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(lineno)d - %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
logger = logging.getLogger(__name__)

engine = create_engine('postgresql://bodastage:password@192.168.99.100/bts')

Session = sessionmaker(bind=engine)
session = Session()

metadata = MetaData()

logger.setLevel(logging.DEBUG)

table = Table('baseline_parameter_config', metadata, autoload=True, autoload_with=engine,schema='live_network')

result = session.query(table).all()

print('    op.bulk_insert(baseline_parameter_config, [')
for mo in result:
    # logger.info(mo)
    mo_pk = mo[1]
    parameter_pk = mo[2]
    tech_pk = mo[6]
    print("        {{'mo_pk': '{}', 'parameter_pk': {}, 'modified_by': 0, 'added_by': 0}},".format(mo_pk, parameter_pk))
print(" ])")

session.close()