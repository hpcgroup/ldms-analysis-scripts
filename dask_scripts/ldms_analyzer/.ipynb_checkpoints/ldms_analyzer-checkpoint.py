import pandas as pd
import sys
import glob
import numpy as np
import datetime

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as PC

import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.patches import Rectangle

import dask
from dask.distributed import Client, get_client
import dask.dataframe as dd
import os

class LDMSAnalyzer:
    def __init__(self):
        scheduler_file = os.path.join(os.environ["PSCRATCH"], "scheduler_file.json")

        dask.config.config["distributed"]["dashboard"]["link"] = "{JUPYTERHUB_SERVICE_PREFIX}proxy/{host}:{port}/status" 

        self.dask_client = Client(scheduler_file=scheduler_file)
        
        self.ldms_data = {}
        self.sacct_data = {}
        
    def read_data(self, ldms_files, sacct_files, metric):
        # Define a consistent schema for LDMS data
        ldms_schema = pa.schema([
            ("#Time", pa.int64()),
            ("ProducerName", pa.string()),
            ("gpu_id", pa.int64()),
            ("gpu_utilization", pa.float64()),  # Ensure gpu_utilization is float64 for consistency
            ("jobid", pa.int64()),
            ("step", pa.int64())
        ])

        # Define a consistent schema for sacct data
        sacct_schema = pa.schema([
            ("jobidraw", pa.int64()),
            ("step", pa.int64()),
            # ("start", pa.int64()),
            # ("end", pa.int64()),
            ("ProducerName", pa.string()),
            ("state", pa.string()),
            ("nnodes", pa.int64()),
            ("JobName", pa.string()),
            ("Account", pa.string()),
            # ("NodeList", pa.string()),
            ("SubmitLine", pa.string())
        ])
        
        self.ldms_data[metric] = dd.read_parquet(ldms_files, schema=ldms_schema)
        self.sacct_data[metric] = dd.read_parquet(sacct_files, schema=sacct_schema)
        
    def get_duration(self, dataframe, seconds=10):
        # Convert start and end times to datetime using Dask's to_datetime
        dataframe['start_time'] = dd.to_datetime(dataframe['start'], unit='s', utc=True)
        dataframe['end_time'] = dd.to_datetime(dataframe['end'], unit='s', utc=True)

        # Calculate duration using Dask operations
        dataframe['duration'] = dataframe['end_time'] - dataframe['start_time']

        # Filter out rows where the duration is less than the specified seconds
        dataframe = dataframe[dataframe['duration'] > pd.Timedelta(seconds=seconds)]

        return dataframe

    def preprocess_data(self, ldms_dataframe, sacct_dataframe, metric, percentage=True):
        """
        Remove jobs that take less than 10 seconds.
        Filter out completed jobs.
        Filter out 'nstaff' and 'nstaff_g'
        """
        # ldms_dataframe[metric]
        # return
        # ldms_dataframe[metric] = ldms_dataframe[metric].astype('float64')
        
        print("Number of LDMS data points before preprocessing: ", ldms_dataframe.index.size.compute())
        ldms_dataframe[metric] = ldms_dataframe[metric].astype('float64')
        
        if percentage:
            if metric == "mem_copy_utilization" or metric == "gpu_utilization":
                ldms_dataframe = ldms_dataframe[ldms_dataframe[metric] <= 100]
            else:
                ldms_dataframe = ldms_dataframe[ldms_dataframe[metric] <= 1]
        print("Number of LDMS data points after percentage filtering: ", ldms_dataframe.index.size.compute())
        
        def check_duplicates(df):
            return df[df.duplicated(keep=False)]

        meta = sacct_dataframe._meta
        duplicates = sacct_dataframe.map_partitions(check_duplicates, meta=meta).compute()
        if not duplicates.empty:
            print("Duplicate rows found in sacct_dataframe:")
            print(duplicates)

#         ldms_unique_keys = ldms_dataframe[['ProducerName', 'jobid', 'step']].drop_duplicates()
#         sacct_unique_keys = sacct_dataframe[['ProducerName', 'jobid', 'step']].drop_duplicates()

#         if len(ldms_unique_keys) < len(ldms_dataframe) or len(sacct_unique_keys) < len(sacct_dataframe):
#             print("Non-unique keys found in one of the DataFrames.")
            
        sacct_dataframe = self.get_duration(sacct_dataframe, 10)
        print("Number of sacct data points in sacct_dataframe after duration filtering: ", sacct_dataframe.index.size.compute())
        
        sacct_dataframe = sacct_dataframe[sacct_dataframe["Account"] != "nstaff_g"]
        sacct_dataframe = sacct_dataframe[sacct_dataframe["Account"] != "nstaff"]
        print("Number of sacct data points in sacct_dataframe after account filtering: ", sacct_dataframe.index.size.compute())
        
        # sacct_dataframe = sacct_dataframe[sacct_dataframe["state"] == "COMPLETED"]

        sacct_dataframe = sacct_dataframe.rename(columns={'jobidraw': 'jobid'})
        sacct_dataframe['jobid'] = sacct_dataframe['jobid'].astype('int64')
        merged_ldms = dd.merge(ldms_dataframe, sacct_dataframe, on=["ProducerName", "jobid", "step"], how='inner')

        print("Number of LDMS data points after preprocessing: ", merged_ldms.index.size.compute())

        return merged_ldms