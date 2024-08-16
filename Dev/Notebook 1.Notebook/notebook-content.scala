// Fabric notebook source

// METADATA ********************

// META {
// META   "kernel_info": {
// META     "name": "synapse_pyspark"
// META   },
// META   "dependencies": {
// META     "lakehouse": {
// META       "default_lakehouse": "88d86d1c-2274-429c-a9b6-2fa93bb7a621",
// META       "default_lakehouse_name": "LH",
// META       "default_lakehouse_workspace_id": "cfd7654a-1ac0-46ba-83e1-7c5abb6dc4c1"
// META     }
// META   }
// META }

// CELL ********************

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType

// METADATA ********************

// META {
// META   "language": "scala",
// META   "language_group": "synapse_pyspark"
// META }

// CELL ********************

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StructType}

// METADATA ********************

// META {
// META   "language": "scala",
// META   "language_group": "synapse_pyspark"
// META }

// CELL ********************

    def flattenStructSchema(schema: StructType, prefix: String = null): Array[org.apache.spark.sql.Column] = {
      schema.fields.flatMap(f => {
        val columnName = if (prefix == null) f.name else (prefix + "." + f.name)

        f.dataType match {
          case st: StructType => flattenStructSchema(st, columnName)
          case _ => Array(col(columnName).as(columnName.replace(".", "_")))
        }
      })
    }

    def explodeArrayColumns(df: DataFrame): DataFrame = {
      val arrayColumns = df.schema.fields.collect {
        case field if field.dataType.isInstanceOf[ArrayType] => field.name
      }

      arrayColumns.foldLeft(df)((tempDf, colName) => tempDf.withColumn(colName, explode_outer(col(colName))))
    }

    def processDataFrame(df: DataFrame): DataFrame = {
      if (df.schema.fields.exists(f => f.dataType.isInstanceOf[ArrayType])) {
        val explodedDf = explodeArrayColumns(df)
        processDataFrame(explodedDf)
      } else if (df.schema.fields.exists(f => f.dataType.isInstanceOf[StructType])) {
        val flattenedDf = df.select(flattenStructSchema(df.schema):_*)
        processDataFrame(flattenedDf)
      } else {
        df
      }
    }

// METADATA ********************

// META {
// META   "language": "scala",
// META   "language_group": "synapse_pyspark"
// META }

// CELL ********************

// Main function to load JSON and process it
def main(args: Array[String]): Unit = {
  // Initialize Spark session
  val spark = SparkSession.builder()
    .appName("Process DataFrame")
    .config("spark.master", "local")
    .getOrCreate()

  // Load the JSON file into a DataFrame
  val df = spark.read.option("multiline", "true").json("Files/Pipeline Run Logs/Processed/logOf_7793d7b7-ba14-497c-84f3-badd08d31bca.json")

  // Process the DataFrame
  val processedDf = processDataFrame(df)

  // Show the processed DataFrame
  display(processedDf)
  processedDf.printSchema()

  // Stop the Spark session
  spark.stop()
}


// METADATA ********************

// META {
// META   "language": "scala",
// META   "language_group": "synapse_pyspark"
// META }

// CELL ********************

main(Array.empty)

// METADATA ********************

// META {
// META   "language": "scala",
// META   "language_group": "synapse_pyspark"
// META }

// CELL ********************


// METADATA ********************

// META {
// META   "language": "scala",
// META   "language_group": "synapse_pyspark"
// META }
