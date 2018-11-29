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


sql2 = """
SELECT DISTINCT 
table_name FROM information_schema."views"
where
table_schema = 'huawei_cm_3g'
"""

result2 = engine.execute(sql2)

view_list = map(lambda x: x[0], result2.fetchall())



for v in view_list:
    # logger.info("view: {}".format(v))

    view = Table(v, metadata, autoload=True, autoload_with=engine, schema='huawei_cm_3g')
    view_columns = map(lambda x: x.name, view.columns)

    # logger.info(view_columns)
    # Check in gexport GSM has MOs with a similar name
    sql = """
    SELECT DISTINCT 
    table_name FROM information_schema."columns"
    where
    table_schema = 'huawei_gexport_wcdma'
    AND 
    (
        table_name like '{0}\_%%' 
        OR 
        table_name = '{0}'
    )
    """.format(v)

    # logger.debug("SQL: {}".format(sql))

    mo_list = []

    try:
        res = engine.execute(sql)
        mo_list = map(lambda x: x[0], res.fetchall())
    except:
        pass




    logger.info(mo_list)

    # MML
    # ##########################################
    sql3 = """
    SELECT DISTINCT 
    table_name FROM information_schema."columns"
    where
    table_schema = 'huawei_mml_umts'
    AND 
    (
        table_name like '{0}\_%%' 
        OR 
        table_name = '{0}'
    )
    """.format(v)

    # logger.debug("SQL: {}".format(sql))

    res3 = engine.execute(sql3)

    mml_mo_list = map(lambda x: x[0], res3.fetchall())


    # #########

    print("""
{0} = ReplaceableObject(
        'huawei_cm_3g."{1}"',
        \"\"\"
                """.format(v, v.upper()))

    print("    SELECT")

    cnt = 0
    for c in view_columns:
        cnt += 1
        if cnt == len(view_columns):
            print("        t1.\"{}\"".format(c))
        else:
            print("        t1.\"{}\",".format(c))

    print("    FROM")
    # print("    {}.\"{}\" t1".format('huawei_cm_2g', v))
    print("    {}.\"{}\" t1".format('huawei_nbi_umts', v))
    print("    ")

    mo_position = 1
    for mo in mo_list:
        mo_position += 1
        table = Table(mo, metadata, autoload=True, autoload_with=engine, schema='huawei_gexport_wcdma')
        columns1 = map(lambda x: x.name, table.columns)
        # logger.info(columns1)


        ne_type_for_gexport = ""
        mo_and_ne_type = mo.split("_")
        if len(mo_and_ne_type) == 2 : ne_type_for_gexport = mo_and_ne_type[1]

        print("UNION")
        print("SELECT")
        cnt = 0
        for c in view_columns:
            # logger.info("parameter: {}".format(c))
            cnt += 1

            if c in columns1:
                # logger.info('t2."{}",'.format(c))

                if cnt == len(view_columns):
                    print("        t{}.\"{}\"".format(mo_position, c))
                else:
                    print("        t{}.\"{}\",".format(mo_position, c))
            elif c == 'neid':
                if cnt == len(view_columns):
                    print("        t{}1.\"SYSOBJECTID\" AS neid".format(mo_position, c))
                else:
                    print("        t{}1.\"SYSOBJECTID\" AS neid,".format(mo_position, c))
            elif c == 'neversion':
                if cnt == len(view_columns):
                    print("        t{}1.\"VERSION\" AS neversion".format(mo_position, c))
                else:
                    print("        t{}1.\"VERSION\" AS neversion,".format(mo_position, c))
            elif c == 'varDateTime':
                if cnt == len(view_columns):
                    print("        t{}1.\"DATETIME\" AS varDateTime".format(mo_position, c))
                else:
                    print("        t{}1.\"DATETIME\" AS varDateTime,".format(mo_position, c))
            elif c == 'netype':
                if cnt == len(view_columns):
                    print("        t{}1.\"NETYPE\" AS ne_type".format(mo_position, c))
                else:
                    print("        t{}1.\"NETYPE\" AS ne_type,".format(mo_position, c))
            elif c == 'FileName':
                if cnt == len(view_columns):
                    print("        t{}1.\"FILENAME\" AS FileName".format(mo_position, c))
                else:
                    print("        t{}1.\"FILENAME\" AS FileName,".format(mo_position, c))
            else:
                # logger.info("NULL AS {},".format(c))
                if cnt == len(view_columns):
                    print("        NULL".format(c))
                else:
                    print("        NULL,".format(c))

        print("FROM")
        print("huawei_gexport_wcdma.\"{}\" t{}".format(mo, mo_position))
        print("""INNER JOIN huawei_gexport_wcdma."SYS_{1}" t{0}1 ON t{0}1."FILENAME" = t{0}."FILENAME\""""
            .format(mo_position, ne_type_for_gexport))
        

    for mo in mml_mo_list:
        mo_position += 1
        table = Table(mo, metadata, autoload=True, autoload_with=engine, schema='huawei_mml_umts')
        columns1 = map(lambda x: x.name, table.columns)
        # logger.info(columns1)

        print("UNION")
        print("SELECT")
        cnt = 0
        for c in view_columns:
            # logger.info("parameter: {}".format(c))
            cnt += 1

            if c in columns1:
                if cnt == len(view_columns):
                    print("        t{}.\"{}\"".format(mo_position, c))
                else:
                    print("        t{}.\"{}\",".format(mo_position, c))
            elif c == 'neid':
                if cnt == len(view_columns):
                    print("        t{}1.\"SYSOBJECTID\" AS neid".format(mo_position, c))
                else:
                    print("        t{}1.\"SYSOBJECTID\" AS neid,".format(mo_position, c))
            elif c == 'neversion':
                if cnt == len(view_columns):
                    print("        t{}.\"BAM_VERSION\" AS neversion".format(mo_position, c))
                else:
                    print("        t{}.\"BAM_VERSION\" AS neversion,".format(mo_position, c))
            elif c == 'module_remark':
                if cnt == len(view_columns):
                    print("        t{}.\"OMU_IP\" AS module_remark".format(mo_position, c))
                else:
                    print("        t{}.\"OMU_IP\" AS module_remark,".format(mo_position, c))
            else:
                if cnt == len(view_columns):
                    print("        NULL".format(c))
                else:
                    print("        NULL,".format(c))

        print("FROM")
        print("huawei_mml_umts.\"{}\" t{}".format(mo, mo_position))
        print("""INNER JOIN huawei_mml_umts."SYS" t{0}1 ON t{0}1."FileName" = t{0}."FileName"
        """.format(mo_position))


    # Close replaceble object
    print("    \"\"\"")
    print("    )")
    # break


print("def upgrade():")
for m in view_list:
    print("    op.create_view({})".format(m))
print("")

print("def downgrade():")
for m in view_list:
    print("    op.drop_view({})".format(m))
print("")
