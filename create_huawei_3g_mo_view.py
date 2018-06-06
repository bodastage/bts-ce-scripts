from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
import time
import timeit
import sys
import os
import logging


logging.basicConfig(format='%(asctime)s - %(levelname)s - %(lineno)d - %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
logger = logging.getLogger(__name__)

engine = create_engine('postgresql://bodastage:password@192.168.99.100/bts')

Session = sessionmaker(bind=engine)
session = Session()

metadata = MetaData()

logger.setLevel(logging.DEBUG)

sql = """
SELECT DISTINCT 
table_name FROM information_schema."columns"
where
table_schema = 'huawei_nbi_umts'
"""

schema_name = 'huawei_nbi_umts'

result = engine.execute(sql)

mo_list = map(lambda x: x[0], result.fetchall())

none_vs_mo_list = []

# print(mo_list)
# logger.info("len(mo_list): {}".format(len(mo_list)))

for mo in mo_list:
    print("""
{0} = ReplaceableObject(
    'huawei_cm_3g."{1}"',
    \"\"\"
            """.format(mo, mo.upper()))

    table = Table(mo, metadata, autoload=True, autoload_with=engine, schema=schema_name)
    columns = map(lambda x: x.name, table.columns)

    print("    SELECT ")

    cnt = 0
    for c in columns:
        cnt += 1
        if cnt == len(columns):
            print("        \"{}\"".format(c))
        else:
            print("        \"{}\",".format(c))

    print("    FROM")
    print("    {}.\"{}\"".format(schema_name, mo))
    print("    ")

    # Close replaceble object
    print("    \"\"\"")
    print("    )")


print("def upgrade():")
for m in mo_list:
    print("    op.create_view({})".format(m))
print("")

print("def downgrade():")
for m in mo_list:
    print("    op.drop_view({})".format(m))
print("")



