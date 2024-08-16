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
// META     },
// META     "environment": {}
// META   }
// META }

// MARKDOWN ********************

// # **Modules**

// CELL ********************

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.ActorMaterializer
import akka.stream.Materializer

// METADATA ********************

// META {
// META   "language": "scala",
// META   "language_group": "synapse_pyspark"
// META }

// CELL ********************

import spray.json._
import DefaultJsonProtocol._

// METADATA ********************

// META {
// META   "language": "scala",
// META   "language_group": "synapse_pyspark"
// META }

// CELL ********************

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}
import com.typesafe.scalalogging.LazyLogging
import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._

// METADATA ********************

// META {
// META   "language": "scala",
// META   "language_group": "synapse_pyspark"
// META }

// CELL ********************

import java.nio.file.{Paths, Files}
import java.nio.charset.StandardCharsets
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

import org.apache.spark.sql.{SparkSession, DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import spark.implicits._
import io.delta.tables._


// METADATA ********************

// META {
// META   "language": "scala",
// META   "language_group": "synapse_pyspark"
// META }

// CELL ********************

import org.json4s._
import org.json4s.jackson.JsonMethods._

// METADATA ********************

// META {
// META   "language": "scala",
// META   "language_group": "synapse_pyspark"
// META }

// MARKDOWN ********************

// # Spark Session

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

// MARKDOWN ********************

// # **Fetch Pipeline Logs to Json File**

// CELL ********************

case class PipelineRunQueryRequest(
  filters: Seq[Map[String, String]] = Seq.empty, 
  orderBy: Seq[Map[String, String]],
  lastUpdatedAfter: String,
  lastUpdatedBefore: String
)

// METADATA ********************

// META {
// META   "language": "scala",
// META   "language_group": "synapse_pyspark"
// META }

// CELL ********************

import spray.json._


object JsonProtocol extends DefaultJsonProtocol {
  implicit val mapFormat: JsonFormat[Map[String, String]] = mapFormat[String, String]
  implicit val pipelineRunQueryRequestFormat: RootJsonFormat[PipelineRunQueryRequest] = jsonFormat4(PipelineRunQueryRequest)
}

// METADATA ********************

// META {
// META   "language": "scala",
// META   "language_group": "synapse_pyspark"
// META }

// CELL ********************

class FabricApiClient(implicit val system: ActorSystem, val materializer: Materializer, ec: ExecutionContext) extends LazyLogging {
  import JsonProtocol._
  import spray.json._

  private val http = Http(system)

  def getAuthToken: String = Try(mssparkutils.credentials.getToken("pbi")).getOrElse {
    logger.error("Failed to get authentication token")
    throw new RuntimeException("Failed to get authentication token")
  }

  def collect(wid: String, pid: String): Future[String] = {
    val url = s"https://api.fabric.microsoft.com/v1/workspaces/$wid/datapipelines/pipelineruns/$pid/queryactivityruns"

    val requestBody = PipelineRunQueryRequest(
      orderBy = Seq(Map("orderBy" -> "ActivityRunStart", "order" -> "DESC")),
      lastUpdatedAfter = "2024-05-22T14:02:04.1423888Z",
      lastUpdatedBefore = "2024-12-31T13:21:27.738Z"
    )

    val entity = HttpEntity(ContentTypes.`application/json`, requestBody.toJson.compactPrint)

    val request = HttpRequest(
      method = HttpMethods.POST,
      uri = url,
      headers = List(Authorization(OAuth2BearerToken(getAuthToken))),
      entity = entity
    )

    logger.info(s"Sending request to $url")

    http.singleRequest(request).flatMap { response =>
      response.status match {
        case StatusCodes.OK => 
          logger.info(s"Request successful. Status: ${response.status}")
          Unmarshal(response.entity).to[String]
        case _ => 
          logger.error(s"Request failed. Status: ${response.status}")
          Future.failed(new RuntimeException(s"Request failed with status ${response.status}"))
      }
    }
    
  }

  def writeJsonToFile(json: String, filename: String): Future[Unit] = Future {
    val outputPath = Paths.get(filename)
    val formattedJson = json.parseJson.prettyPrint
    Files.write(outputPath, formattedJson.getBytes(StandardCharsets.UTF_8))
    logger.info(s"Output written to ${outputPath.toAbsolutePath}")
  } recover {
    case ex =>
      logger.error(s"Failed to write JSON to file: ${ex.getMessage}")
  }
}

// METADATA ********************

// META {
// META   "language": "scala",
// META   "language_group": "synapse_pyspark"
// META }

// CELL ********************

object FabricApiClientApp extends LazyLogging {

  implicit val system: ActorSystem = ActorSystem("fabric-api-client-system")
  implicit val materializer: ActorMaterializer = ActorMaterializer()(system)
  implicit val ec: ExecutionContext = system.dispatcher

  def main(args: Array[String]): Unit = {
    val client = new FabricApiClient()
    
    try {

      val tdf = spark.read.table("Pipeline_Logs")

      val pids: Seq[String] = tdf.select("Pipeline_Run_ID").distinct().as[String].collect().toSeq
      
      val wid: String = mssparkutils.runtime.context.get("currentWorkspaceId") match {
        case Some(value: String) => value
        case Some(_) => throw new RuntimeException("Current Workspace ID is not a String")
        case None => throw new RuntimeException("Failed to get Current Workspace ID")
      }

      val futures: Seq[Future[Unit]] = 
        if (pids == null || pids.isEmpty) {
          logger.info("No pids to process")
          Seq.empty[Future[Unit]]
        } else {
          pids.map { pid =>
          client.collect(wid, pid).flatMap { result =>
          client.writeJsonToFile(result, s"/lakehouse/default/Files/Pipeline Run Logs/Not-Processed/logOf_$pid.json")
          }.recover {
            case ex => 
              logger.error(s"Failed to process pid $pid: ${ex.getMessage}")
          }
          }
        }

      if (futures.nonEmpty) {
        Await.result(Future.sequence(futures), 10.minutes)
        logger.info("All API calls completed successfully")
        println("All API calls completed successfully")
      }
    } catch {
      case ex: Exception =>
        logger.error(s"An unexpected error occurred: ${ex.getMessage}")
    } finally {
      val terminationFuture: Future[Unit] = system.terminate().map { _ =>
            println("ActorSystem terminated.")
        }

        try {
            Await.result(terminationFuture, 1.minute)
            println("Shutdown complete.")
          } catch {
            case ex: Exception =>
              println(s"Shutdown failed: ${ex.getMessage}")
          }
    }
  }
}

// METADATA ********************

// META {
// META   "language": "scala",
// META   "language_group": "synapse_pyspark"
// META }

// CELL ********************

FabricApiClientApp.main(Array.empty[String])

// METADATA ********************

// META {
// META   "language": "scala",
// META   "language_group": "synapse_pyspark"
// META }

// MARKDOWN ********************

// # **From Json File to Delta able**

// CELL ********************

trait DataProcessor {
    def flattenStructSchema(schema: StructType, prefix: String = null): Array[org.apache.spark.sql.Column]
    def explodeArrayColumns(df: DataFrame): DataFrame
    def processDataFrame(df: DataFrame): DataFrame
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

  def process(filename: String): DataFrame = {
    logger.info("Processing DataFrame")

    val df = spark.read.option("multiline", "true").json(s"Files/Pipeline Run Logs/Not-Processed/$filename")

    val processedDf = processDataFrame(df)

    processedDf
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
      val spark = SparkSessionManager.getOrCreateSparkSession("Log APP")

      try {

        logger.info("Loading Files")

        val folderPath = "/lakehouse/default/Files/Pipeline Run Logs/Not-Processed"
        val fileList = getFiles(folderPath)

        println(fileList)

        if (fileList.nonEmpty){

            val processor: DataProcessor = new jsonToDF()

            fileList.foreach { file =>
              try {
                println(s"Processing file: $file")

                val processedDF: DataFrame = processor.process(file)

                processedDF.write.format("delta").mode("append").option("mergeSchema", "true").save("Tables/Pipeline_Run_Logs")

                updateSync(file, 
                  Timestamp.valueOf(LocalDateTime.now()), 
                  processedDF.count(), 
                  processedDF.agg(min("activityRunStart")).first().getString(0), 
                  processedDF.agg(max("activityRunEnd")).first().getString(0))
                mssparkutils.fs.mv(s"Files/Pipeline Run Logs/Not-Processed/$file", s"Files/Pipeline Run Logs/Processed/$file", true)
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
          StructField("activityRunStart", StringType, true),
          StructField("activityRunEnd", StringType, true)
        ))


        val newRow = spark.createDataFrame(Seq(
          (fileName, processedTime, processedRowCount, startDate, endDate)
        )).toDF("File_Name", "Processed_Time", "Processed_Row_Count", "activityRunStart", "activityRunEnd")

          newRow.write.format("delta").mode("append").option("mergeSchema", "true").save("Tables/PipelineLog_Sync_Data")

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

// MARKDOWN ********************

// #

// CELL ********************


// METADATA ********************

// META {
// META   "language": "scala",
// META   "language_group": "synapse_pyspark"
// META }
