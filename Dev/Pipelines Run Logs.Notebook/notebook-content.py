# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "5a47e09d-bc8e-4b86-af09-352b65ccb0a9",
# META       "default_lakehouse_name": "LH_Fabric",
# META       "default_lakehouse_workspace_id": "99946812-f54d-4553-84c2-9375c0f5e8d1"
# META     }
# META   }
# META }

# CELL ********************

import json
import pandas as pd
import requests

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, current_timestamp, to_date

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def getSpark():
    spark = SparkSession.builder \
            .appName("Log Analysis") \
            .getOrCreate()
    return spark

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def is_not_array_or_dict(series):
    return all(not isinstance(x, (list, dict)) for x in series)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def collect(wid, pid):
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{wid}/datapipelines/pipelineruns/{pid}/queryactivityruns"

    payload = json.dumps({
        "filters": [],
        "orderBy": [
        {
            "orderBy": "ActivityRunStart",
            "order": "DESC"
        }
        ],
        "lastUpdatedAfter": "2024-05-22T14:02:04.1423888Z",
        "lastUpdatedBefore": "2024-12-31T13:21:27.738Z"
    })
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f"Bearer {mssparkutils.credentials.getToken('pbi')}"
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    n = pd.json_normalize(json.loads(response.text), max_level = 10)

    if n.empty:
        return pd.DataFrame()


    non_array_dict_columns = [col for col in n.columns if is_not_array_or_dict(n[col])]

    n = n[non_array_dict_columns]

    n.columns = n.columns.map(str)
    n.columns = n.columns.str.replace('.', '_')
    n.columns = n.columns.str.replace('-', '_')

    j = [col for col in n.columns if col.startswith('input_storedProcedureParameters_') or col.startswith('output_message') or col.startswith('output_status')]


    n = n.drop(columns = j)

    return n

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def getPID(spark):
    pipeline = spark \
            .read \
            .format("delta") \
            .table("Analytic_WH_PipelineRuns_Details")

    df = pipeline.select("runID").filter(to_date(pipeline["Time"]) <= current_date())
    pm = spark.sql("SELECT DISTINCT pipelineRunId FROM LH_Fabric.Pipeline_Run_Logs ")

    h = list(df.join(pm, pm.pipelineRunId == df.runID, how = "leftanti").toPandas()['runID'])
    j = list(filter(lambda x: x is not None, h))

    return h

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

if __name__ == "__main__":

    spark = getSpark()

    #pid = getPID(spark)

    pid = ''

    wid = mssparkutils.runtime.context['currentWorkspaceId']


    df = pd.DataFrame()

    #for i in pid:
    n = collect(wid, i)
    df = pd.concat([df, n])

    display(df.reset_index())
    #display(spark.createDataFrame(df.reset_index()))
    spark.createDataFrame(df.reset_index()).write.format("delta").mode("append").option("mergeschema","true").save("Tables/Pipeline_Run_Logs")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
