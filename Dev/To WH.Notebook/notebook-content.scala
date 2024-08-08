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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// METADATA ********************

// META {
// META   "language": "scala",
// META   "language_group": "synapse_pyspark"
// META }

// CELL ********************

val spark  = SparkSession.builder()
              .appName("M-Query to Scala Spark")
              .getOrCreate()

// METADATA ********************

// META {
// META   "language": "scala",
// META   "language_group": "synapse_pyspark"
// META }

// CELL ********************

val mainTable = spark.read.table("Earthquake_Data")

// METADATA ********************

// META {
// META   "language": "scala",
// META   "language_group": "synapse_pyspark"
// META }

// CELL ********************

val result = mainTable
  .select("magnitudeType")
  .distinct()
  .orderBy(asc("magnitudeType"))
  .filter(col("magnitudeType").isNotNull && col("magnitudeType") =!= "")
  .withColumn("MagnitudeID", monotonically_increasing_id() + 1)
  .select("MagnitudeID", "magnitudeType")

// METADATA ********************

// META {
// META   "language": "scala",
// META   "language_group": "synapse_pyspark"
// META }

// CELL ********************

display(result)

// METADATA ********************

// META {
// META   "language": "scala",
// META   "language_group": "synapse_pyspark"
// META }

// CELL ********************

val geodata = mainTable
  .select("timezone", "geometryType", "longitude", "latitude", "id")
  .select("id", "longitude", "latitude", "geometryType", "timezone")

// METADATA ********************

// META {
// META   "language": "scala",
// META   "language_group": "synapse_pyspark"
// META }

// CELL ********************

display(geodata)

// METADATA ********************

// META {
// META   "language": "scala",
// META   "language_group": "synapse_pyspark"
// META }

// CELL ********************

val eventdetails = mainTable
  .select("id", "sources", "ids", "url", "detailUrl")

// METADATA ********************

// META {
// META   "language": "scala",
// META   "language_group": "synapse_pyspark"
// META }

// CELL ********************

display(eventdetails)

// METADATA ********************

// META {
// META   "language": "scala",
// META   "language_group": "synapse_pyspark"
// META }

// CELL ********************

val fact = mainTable
  .select("id", "magnitude", "felt", "cdi", "mmi", "significance", "nst", "dmin", "rms","magnitudeType")

// METADATA ********************

// META {
// META   "language": "scala",
// META   "language_group": "synapse_pyspark"
// META }

// CELL ********************

display(fact)

// METADATA ********************

// META {
// META   "language": "scala",
// META   "language_group": "synapse_pyspark"
// META }

// CELL ********************

val ffact = fact
  .join(result, Seq("magnitudeType"), "left_outer")
  .select("id", "magnitude", "felt", "cdi", "mmi", "significance", "nst", "dmin", "rms", "MagnitudeID")

// METADATA ********************

// META {
// META   "language": "scala",
// META   "language_group": "synapse_pyspark"
// META }

// CELL ********************

display(ffact)

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
