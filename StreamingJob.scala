package io.keepcoding.spark.exercise.streaming

import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.Timestamp
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

case class AntennaMessage(timestamp: Timestamp, id: String, metric: String, value: Long)

trait StreamingJob {

  val spark: SparkSession

  def readFromKafka(kafkaServer: String, topic: String): DataFrame

  def parserJsonData(dataFrame: DataFrame): DataFrame

//  def readAntennaMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame

//  def enrichAntennaWithMetadata(antennaDF: DataFrame, metadataDF: DataFrame): DataFrame

  //def computeDevicesCountByCoordinates(dataFrame: DataFrame): DataFrame

  def computeBytesSUMbyAntenna(dataFrame: DataFrame): DataFrame

  def computeBytesSUMbyUser(dataFrame1: DataFrame): DataFrame

  def computeBytesSUMbyAPP(dataFrame1: DataFrame): DataFrame

  def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Future[Unit]

  def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Future[Unit]

  def run(args: Array[String]): Unit = {
    val Array(kafkaServer, topic, jdbcUri, jdbcMetadataTable, aggJdbcTable, jdbcUser, jdbcPassword, storagePath) = args
    println(s"Running with: ${args.toSeq}")

    val kafkaDF = readFromKafka(kafkaServer, topic)
    val parsedDF = parserJsonData(kafkaDF)
    //val metadataDF = readAntennaMetadata(jdbcUri, jdbcMetadataTable, jdbcUser, jdbcPassword)
    //val antennaMetadataDF = enrichAntennaWithMetadata(parsedDF, metadataDF)
    val storageFuture = writeToStorage(parsedDF, storagePath)
    //val aggByCoordinatesDF = computeDevicesCountByCoordinates(antennaMetadataDF) // Ejercicio Final
    val countByLocation = computeBytesSUMbyAntenna(parsedDF) // Práctica
    val countByUser = computeBytesSUMbyUser(parsedDF) // Práctica
    val countByApp = computeBytesSUMbyAPP(parsedDF) // Práctica
    val jdbcFuture = writeToJdbc(countByLocation, jdbcUri, "bytes", jdbcUser, jdbcPassword)
    val jdbcFuture1 = writeToJdbc(countByUser, jdbcUri, "bytes", jdbcUser, jdbcPassword)
    val jdbcFuture2 = writeToJdbc(countByApp, jdbcUri, "bytes", jdbcUser, jdbcPassword)

   Await.result(
      Future.sequence(Seq(storageFuture, jdbcFuture, jdbcFuture1, jdbcFuture2)),
      Duration.Inf)

    spark.close()
  }

}
