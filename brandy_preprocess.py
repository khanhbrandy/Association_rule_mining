"""
Created on 2019-03-10
Creator: khanh.brandy

"""
import pandas as pd
import numpy as np
import time
import ibm_db
import ibm_db_dbi
from scipy.sparse import csr_matrix
from pandas.api.types import CategoricalDtype

class Preprocessor:
    def __init__(self):
        pass

    def db2_connect(self, dsn_database, dsn_hostname, dsn_port, dsn_protocol, dsn_uid, dsn_pwd):
        # Create connection
        dsn = (
        "DRIVER={{IBM DB2 ODBC DRIVER}};"
        "DATABASE={0};"
        "HOSTNAME={1};"
        "PORT={2};"
        "PROTOCOL=TCPIP;"
        "UID={3};"
        "PWD={4};").format(dsn_database, dsn_hostname, dsn_port, dsn_uid, dsn_pwd)

        # Get data interest LV4 from DB2
        db2_conn = ibm_db.connect(dsn, "", "")
        db2_sql = '''
        --- SQL query here
        '''
        pconn = ibm_db_dbi.Connection(db2_conn)
        db2_df = pd.read_sql(db2_sql, pconn)
        return db2_df

    def data_preprocess(self, data,level):
        # Convert to sparse matrix
        userid_c = pd.Categorical(sorted(data['USER_ID'].unique()), ordered=True)
        level_c = pd.Categorical(sorted(data[level].unique()), ordered=True)
        row = data['USER_ID'].astype(userid_c).cat.codes
        col = data[level].astype(level_c).cat.codes
        sparsed_df = csr_matrix((np.ones(data.shape[0]), (row, col)), shape=(userid_c.categories.size, level_c.categories.size))
        # Convert to sparse df
        dense_df=pd.SparseDataFrame(sparsed_df,
                                    index=userid_c.categories,
                                    columns=level_c.categories,
                                    default_fill_value=None)
        dense_df.reset_index(inplace=True)
        dense_df.rename(columns={'index':'USER_ID'},inplace=True)
        return dense_df


        
