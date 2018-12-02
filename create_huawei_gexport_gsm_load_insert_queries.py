# Generate load query for Huawei GExport GSM

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


# Get load tables
sql = """
select distinct table_name from
information_schema.columns
where 
table_schema = 'huawei_cm_2g'
"""

result = engine.execute(sql)
mo_list = list( map(lambda x: x[0], result.fetchall()) )

mo_param_list = {}

for mo in mo_list:
    sql2 = """
    SELECT column_name FROM
    information_schema.columns
    WHERE 
    table_schema = 'huawei_cm_2g'
    AND table_name = '{}'
    ORDER BY ordinal_position asc
    """.format(mo)

    result2 = engine.execute(sql2)

    column_list = list(map(lambda x: x[0], result2.fetchall()))
    mo_param_list[mo] = column_list

    # print(mo_param_list)


# Get gexport mos
for mo in mo_list:
    sql3 = """
    select distinct table_name from
    information_schema.columns
    where 
    table_schema = 'huawei_gexport_gsm'
    AND ( table_name  = '{0}' OR table_name = 'G' || '{0}' )
    LIMIT 1
    """.format(mo)

    result3 = engine.execute(sql3)
    if result3 is None: continue
    mo3 = result3.fetchone()[0]

    # print("mo3: {}".format(mo3))

    # Get parameters in gexport MO
    sql4 = """
    SELECT column_name from
    information_schema.columns
    WHERE 
    table_schema = 'huawei_gexport_gsm'
    AND table_name = '{}'
    ORDER BY ordinal_position asc
    """.format(mo3)
    result4 = engine.execute(sql4)
    column_list4 = list(map(lambda x: x[0], result4.fetchall()))

    insert_query = ""
    insert_query += " INSERT INTO huawei_cm_2g.\"{}\"  ".format(mo)

    insert_query += " SELECT t2.pk as \"LOADID\","

    cnt = 0
    mo_columns  = mo_param_list[mo]
    num_of_params = len(mo_columns)

    for c in mo_columns:
        cnt += 1

        if c == 'LOADID': continue

        comma = ""
        if cnt < num_of_params :
            comma = ","

        if c in column_list4:
            insert_query += "    t.\"{0}\" AS \"{0}\" {1}".format(c, comma)
        else:
            insert_query += "    NULL AS \"{0}\" {1}".format(c, comma)

    insert_query += "    FROM"
    insert_query += "    huawei_gexport_gsm.\"{}\" t ".format(mo)
    insert_query += "    CROSS JOIN cm_loads t2  WHERE t2.is_current_load = true ".format(mo3)

    print("{{'file_format': 'huawei_gexport_gsm', 'mo': '{}', 'insert_query': '{}'}},".format(mo, insert_query))
    # sys.exit(0)

