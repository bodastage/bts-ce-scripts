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
table_schema = 'zte_bulkcm_lte'
"""

schema_name = 'zte_bulkcm_lte'

result = engine.execute(sql)

mo_list = map(lambda x: x[0], result.fetchall())

none_vs_mo_list = []

# print(mo_list)
# logger.info("len(mo_list): {}".format(len(mo_list)))

while len(mo_list) > 0 :
    mo = mo_list[0]

    if mo in ['fileHeader','fileFooter', 'configData','bulkCmConfigDataFile']:
        mo_list.remove(mo)
        continue

    # original_mo = mo = mo_list.pop()
    # logger.info("{}".format(mo))

    if 'vsData' in mo:
        vs_mo = mo
        none_vs_mo = mo.replace('vsData','')

    else:  # MO does not start with vsData
        vs_mo = 'vsData' + mo
        none_vs_mo = mo

    # logger.info("vs_mo: {} none_vs_mo: {}".format(vs_mo, none_vs_mo))

    # logger.info("Checking if there is a {} mo".format(none_vs_mo) )

    if none_vs_mo in mo_list and vs_mo in mo_list:

        # Remove from list


        # logger.info("mo: {}".format(mo))
        # logger.info("none_vs_mo: {}".format(none_vs_mo))
        # logger.info("None Vendor specific mo exists called {}".format(none_vs_mo))
        table1 = Table(vs_mo, metadata, autoload=True, autoload_with=engine, schema=schema_name)
        table2 = Table(none_vs_mo, metadata, autoload=True, autoload_with=engine, schema=schema_name)

        columns1 = map(lambda x: x.name, table1.columns)
        columns2 = map(lambda x: x.name, table2.columns)

        intersection = list(set(columns2) & set(columns1))

        # column1_minus_intersection = list(set(columns1) - set(intersection))
        # column1_minus_intersection = [item for item in intersection if item not in set(columns1)]
        column1_minus_intersection = [i for i in columns1 if not i in intersection ]

        # Only pick fields that are varDateTime, configData_dnPrefix or  that ends with _id
        intersection = filter(lambda x: '_id' in x or x in ['varDateTime', 'configData_dnPrefix','FileName'], intersection)

        # column1_minus_intersection = list(set(columns1) - set(intersection))
        # column1_minus_intersection = [item for item in intersection if item not in set(columns1)]

        # columns = list(set(columns1 + columns2))
        columns = columns2 + column1_minus_intersection

        # logger.info(columns1)
        # logger.info(columns2)
        #
        # logger.info(columns)
        # logger.info(intersection)
        # logger.info(column1_minus_intersection)

        print("""
{0} = ReplaceableObject(
    'zte_cm_4g."{0}"',
    \"\"\"
        """.format(none_vs_mo))

        # print("CREATE OR REPLACE VIEW zte_cm_4g.\"{}\" AS".format(none_vs_mo))
        print("SELECT ")

        cnt = 0
        for c in columns2:
            cnt+=1
            print("t1.\"{}\",".format(c))


        cnt = 0
        for c in column1_minus_intersection:
            cnt+=1
            if cnt == len(column1_minus_intersection):
                print("t2.\"{}\"".format(c))
            else:
                print("t2.\"{}\",".format(c))

        print("FROM")
        print("{}.\"{}\" t1".format(schema_name, none_vs_mo ))
        print("INNER JOIN {}.\"{}\" t2".format(schema_name, vs_mo))

        cnt = 0
        for col in intersection:
            cnt+=1
            if cnt == 1:
                print(" ON t1.\"{0}\" = t2.\"{0}\" ".format(col))
            else:
                print(" AND t1.\"{0}\" = t2.\"{0}\" ".format(col))

        print("")

        # Close replaceble object
        print("\"\"\"")
        print(")")

    else:

        # logger.info("None Vendor specific mo called {} does not exist".format(none_vs_mo))

        table = Table(mo, metadata, autoload=True, autoload_with=engine, schema=schema_name)

        columns = map( lambda x: x.name, table.columns)
        # columns = map(lambda x: x.replace(mo,'') , table.columns)
        # logger.info(table.columns)
        # columns = list(table.columns)
        # logger.info(columns)
        # logger.info(type(columns))
        # logger.info( map(lambda x: x.replace(mo,'') , columns) )
        print("""
{0} = ReplaceableObject(
    'zte_cm_4g."{0}"',
    \"\"\"
        """.format(none_vs_mo))

        # print("    CREATE OR REPLACE VIEW zte_cm_4g.\"{}\" AS".format(none_vs_mo))
        print("    SELECT ")

        cnt = 0
        for c in columns:
            cnt+=1
            if cnt == len(columns):
                print("        \"{}\"".format(c))
            else:
                print("        \"{}\",".format(c))

        print("    FROM")
        print("    {}.\"{}\"".format(schema_name, mo ))
        print("    ")

        # Close replaceble object
        print("    \"\"\"")
        print("    )")

    if none_vs_mo in mo_list: mo_list.remove(none_vs_mo)
    if vs_mo in mo_list: mo_list.remove(vs_mo)
    none_vs_mo_list.append(none_vs_mo)


print("def upgrade():")
for m in none_vs_mo_list:
    print("    op.create_view({})".format(m))
print("")

print("def downgrade():")
for m in none_vs_mo_list:
    print("    op.drop_view({})".format(m))
print("")



