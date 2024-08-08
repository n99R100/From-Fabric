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

import org.apache.spark.sql.{SparkSession, DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import spark.implicits._
import io.delta.tables._
import java.io.File
import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

// METADATA ********************

// META {
// META   "language": "scala",
// META   "language_group": "synapse_pyspark"
// META }

// CELL ********************

object SparkSessionManager {
  
  private val logger = org.apache.log4j.LogManager.getLogger(getClass.getName)

  private var sparkSession: Option[SparkSession] = None

  def getOrCreateSparkSession(appName: String = "Spark Session App"): SparkSession = {

    sparkSession.getOrElse {

        logger.info(s"Creating new SparkSession with appName: $appName")

        val newSession: SparkSession = SparkSession.builder()
            .appName(appName)
            .getOrCreate()
        
        newSession.conf.set("spark.ms.autotune.enabled", "true")
        newSession.conf.set("spark.sql.parquet.vorder.enabled", "true")
        newSession.conf.set("spark.microsoft.delta.optimizeWrite.enabled", "true")
        sparkSession = Some(newSession)
        newSession
    }
  }

  def closeSparkSession(): Unit = {

    sparkSession.foreach { session =>
      logger.info("Closing SparkSession")
      mssparkutils.session.stop()
    }
    sparkSession = None
  }
}

// METADATA ********************

// META {
// META   "language": "scala",
// META   "language_group": "synapse_pyspark"
// META }

// CELL ********************

trait DataProcessor {
  def process(filename: String): DataFrame
}

// METADATA ********************

// META {
// META   "language": "scala",
// META   "language_group": "synapse_pyspark"
// META }

// CELL ********************

class jsonToDF extends DataProcessor {
  private val logger = org.apache.log4j.LogManager.getLogger(getClass.getName)

  def process(filename: String): DataFrame = {
    logger.info("Processing DataFrame")
    val df: DataFrame = spark.read.option("multiline", "true").json(s"abfss://cfd7654a-1ac0-46ba-83e1-7c5abb6dc4c1@onelake.dfs.fabric.microsoft.com/88d86d1c-2274-429c-a9b6-2fa93bb7a621/Files/Earthquake/$filename")

    val flattenedDF: DataFrame = df
                    .select(
                        col("type").as("featureCollectionType"),
                        col("metadata.generated").as("metadataGenerated"),
                        col("metadata.url").as("metadataUrl"),
                        col("metadata.title").as("metadataTitle"),
                        col("metadata.status").as("metadataStatus"),
                        col("metadata.api").as("metadataApi"),
                        col("metadata.count").as("metadataCount"),
                        explode(col("features")).as("feature")
                    )
                    .select(
                        col("feature.type").as("featureType"),
                        col("feature.properties.mag").as("magnitude"),
                        col("feature.properties.place").as("place"),
                        (col("feature.properties.time") / 1000).cast("long").as("timeInSeconds"),
                        (col("feature.properties.updated") / 1000).cast("long").as("updatedInSeconds"),
                        col("feature.properties.tz").cast("string").as("timezone"),
                        col("feature.properties.url").as("url"),
                        col("feature.properties.detail").as("detailUrl"),
                        col("feature.properties.felt").cast("string").as("felt"),
                        col("feature.properties.cdi").as("cdi"),
                        col("feature.properties.mmi").as("mmi"),
                        col("feature.properties.alert").as("alert"),
                        col("feature.properties.status").as("status"),
                        col("feature.properties.tsunami").as("tsunami"),
                        col("feature.properties.sig").as("significance"),
                        col("feature.properties.net").as("network"),
                        col("feature.properties.code").as("code"),
                        col("feature.properties.ids").as("ids"),
                        col("feature.properties.sources").as("sources"),
                        col("feature.properties.types").as("types"),
                        col("feature.properties.nst").as("nst"),
                        col("feature.properties.dmin").as("dmin"),
                        col("feature.properties.rms").as("rms"),
                        col("feature.properties.gap").as("gap"),
                        col("feature.properties.magType").as("magnitudeType"),
                        col("feature.properties.title").as("featureTitle"),
                        col("feature.geometry.type").as("geometryType"),
                        col("feature.geometry.coordinates").getItem(0).as("longitude"),
                        col("feature.geometry.coordinates").getItem(1).as("latitude"),
                        col("feature.geometry.coordinates").getItem(2).as("depth"),
                        col("feature.id").as("id")
                    )
                    .withColumn("time", date_format(from_unixtime(col("timeInSeconds")), "yyyy-MM-dd HH:mm:ss"))
                    .withColumn("updated", date_format(from_unixtime(col("updatedInSeconds")), "yyyy-MM-dd HH:mm:ss"))
                    .drop("timeInSeconds", "updatedInSeconds")
    flattenedDF
  }
}

// METADATA ********************

// META {
// META   "language": "scala",
// META   "language_group": "synapse_pyspark"
// META }

// CELL ********************

object SparkApp {

  private val logger = org.apache.log4j.LogManager.getLogger(getClass.getName)

  def main: Unit = {

    try {
      logger.info("Starting SparkApp")
      val spark = SparkSessionManager.getOrCreateSparkSession("APP")

      try {

        logger.info("Loading Files")

        val folderPath = "/lakehouse/default/Files/Earthquake"
        val fileList = getFiles(folderPath)

        println(fileList)

        if (fileList.nonEmpty){

            val processor: DataProcessor = new jsonToDF()

            fileList.foreach { file =>
              try {
                println(s"Processing file: $file")
                val processedDF: DataFrame = processor.process(file)
                processedDF.write.format("delta").mode("append").option("mergeSchema", "true").save("Tables/Earthquake_Data")
                updateSync(file, 
                  Timestamp.valueOf(LocalDateTime.now()), 
                  processedDF.count(), 
                  processedDF.agg(max("time")).first().getString(0), 
                  processedDF.agg(min("time")).first().getString(0))
                mssparkutils.fs.mv(s"Files/Earthquake/$file", s"Files/History/$file", true)
              } catch {
                case e: Exception =>
                  println(s"Error processing file $file: ${e.getMessage}")
                  e.printStackTrace()
              }
            }

        }

        } catch {
            case e: Exception =>
            logger.error(s"An error occurred during data processing: ${e.getMessage}", e)
        }

    } catch {
      case e: Exception =>
        logger.error(s"An error occurred while creating SparkSession: ${e.getMessage}", e)
    } finally {
      logger.info("Cleaning up resources")
      try {
        SparkSessionManager.closeSparkSession()
        mssparkutils.session.stop()
      } catch {
        case e: Exception =>
          logger.error(s"An error occurred while closing SparkSession: ${e.getMessage}", e)
      }
      logger.info("SparkApp finished")
    }
  }

    def getFiles(path: String): List[String] = {
        val dir = new File(path)

        if (dir.exists && dir.isDirectory) {

            val jsonFiles = dir.listFiles
                .filter(file => file.isFile && file.getName.endsWith(".json"))
                .map(_.getName)

            jsonFiles.toList

            } else {
            
            List.empty[String]
        }
    }

    def updateSync(
        fileName: String,
        processedTime: Timestamp,
        processedRowCount: Long,
        startDate: String,
        endDate: String
      ): Unit = {


        val schema = StructType(Array(
          StructField("File_Name", StringType, true),
          StructField("Processed_Time", TimestampType, true),
          StructField("Processed_Row_Count", LongType, true),
          StructField("Start_Date", StringType, true),
          StructField("End_Date", StringType, true)
        ))


        val newRow = spark.createDataFrame(Seq(
          (fileName, processedTime, processedRowCount, startDate, endDate)
        )).toDF("File_Name", "Processed_Time", "Processed_Row_Count", "Start_Date", "End_Date")

          newRow.write.format("delta").mode("append").option("mergeSchema", "true").save("Tables/Earthquake_Sync_Data")

          println(s"Processed: $fileName")

      }

}

// METADATA ********************

// META {
// META   "language": "scala",
// META   "language_group": "synapse_pyspark"
// META }

// CELL ********************

SparkApp.main

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
