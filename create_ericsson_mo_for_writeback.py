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
table_schema = 'ericsson_bulkcm'
"""

schema_name = 'ericsson_bulkcm'

result = engine.execute(sql)

mo_list = map(lambda x: x[0], result.fetchall())

none_vs_mo_list = []

while len(mo_list) > 0 :
    mo = mo_list[0]

    if mo in ['fileHeader','fileFooter', 'configData','bulkCmConfigDataFile']:
        mo_list.remove(mo)
        continue

    # logger.info("mo: {}".format(mo))

    if 'vsData' in mo:
        vs_mo = mo
        none_vs_mo = mo.replace('vsData','')
    else:  # MO does not start with vsData
        vs_mo = 'vsData' + mo
        none_vs_mo = mo


    if none_vs_mo in mo_list and vs_mo in mo_list:
        vs_mo_table = Table(vs_mo, metadata, autoload=True, autoload_with=engine, schema=schema_name)
        none_vs_mo_table = Table(none_vs_mo, metadata, autoload=True, autoload_with=engine, schema=schema_name)

        vs_mo_columns = map(lambda x: x.name, vs_mo_table.columns)
        none_vs_mo_columns = map(lambda x: x.name, none_vs_mo_table.columns)

        print """
class {0}(ManagedObject, BulkCMExtras):
    def __init__(self, **kwargs):
        ManagedObject.__init__(self, '{0}',""".format(none_vs_mo)

        mo_param_start = False
        for parameter in none_vs_mo_columns:
            if none_vs_mo + "_id" == parameter:
                mo_param_start = True
                print """         id=Parameter(name='id', is_attr=True),"""
                continue

            if mo_param_start is False: continue
            print """         {0}=Parameter(name='{0}'),""".format(parameter)

        mo_param_start = False
        for parameter in vs_mo_columns:
            if vs_mo + "_id" == parameter:
                mo_param_start = True
                continue

            if mo_param_start is False: continue
            print """         {0}=Parameter(name='{0}', is_vendor_specific=True, ns_prefix='es'),""".format(parameter)
        print """        )
        
        BulkCMExtras.__init__(self)

        # Set the values
        for k, v in kwargs.items():
            self.set_parameter_value(k, v)

        self.ns_prefix = 'xn'
        
        """

        # logger.info(vs_mo_columns)
        # logger.info(none_vs_mo_columns)

        # break

        if none_vs_mo not in mo_list and vs_mo in mo_list:
            vs_mo_table = Table(vs_mo, metadata, autoload=True, autoload_with=engine, schema=schema_name)
            # none_vs_mo_table = Table(none_vs_mo, metadata, autoload=True, autoload_with=engine, schema=schema_name)

            vs_mo_columns = map(lambda x: x.name, vs_mo_table.columns)
            # none_vs_mo_columns = map(lambda x: x.name, none_vs_mo_table.columns)

            print """
    class {0}(ManagedObject, BulkCMExtras):
        def __init__(self, **kwargs):
            ManagedObject.__init__(self, '{0}',""".format(none_vs_mo)

            mo_param_start = False
            for parameter in vs_mo_columns:
                if vs_mo + "_id" == parameter:
                    mo_param_start = True
                    print """         id=Parameter(name='id', is_attr=True),"""
                    continue

                if mo_param_start is False: continue
                print """         {0}=Parameter(name='{0}', is_vendor_specific=True, ns_prefix='es'),""".format(
                    parameter)
            print """        )

            BulkCMExtras.__init__(self)

            # Set the values
            for k, v in kwargs.items():
                self.set_parameter_value(k, v)

            self.ns_prefix = 'xn'

            """

            # logger.info(vs_mo_columns)
            # logger.info(none_vs_mo_columns)

            # break

    if none_vs_mo in mo_list: mo_list.remove(none_vs_mo)
    if vs_mo in mo_list: mo_list.remove(vs_mo)
    none_vs_mo_list.append(none_vs_mo)