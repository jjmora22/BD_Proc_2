package io.keepcoding.spark.exercise.batch

import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.Timestamp
import java.time.OffsetDateTime

case class AntennaMessage(year: Int, month: Int, day: Int, hour: Int, timestamp: Timestamp, id: String, metric: String, value: Long)

trait BatchJob {

  val spark: SparkSession

  def readFromStorage(storagePath: String, filterDate: OffsetDateTime): DataFrame

  def readAntennaMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame

  def enrichAntennaWithMetadata(antennaDF: DataFrame, metadataDF: DataFrame): DataFrame

  def computeBytesSUMbyAntenna(dataFrame: DataFrame): DataFrame

  def computeBytesSUMbyApp(dataFrame: DataFrame): DataFrame

  def computeBytesSUMbyUser(dataFrame: DataFrame): DataFrame

  def computeBytesExceedQuota(dataFrame: DataFrame): DataFrame

  def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Unit

  def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Unit

  def run(args: Array[String]): Unit = {
    val Array(filterDate, storagePath, jdbcUri, jdbcMetadataTable, aggJdbcTable, aggJdbcErrorTable, aggJdbcPercentTable, jdbcUser, jdbcPassword) = args
    println(s"Running with: ${args.toSeq}")

    val parquetDF = readFromStorage(storagePath, OffsetDateTime.parse(filterDate))
    val metadataDF = readAntennaMetadata(jdbcUri, jdbcMetadataTable, jdbcUser, jdbcPassword)
    val antennaMetadataDF = enrichAntennaWithMetadata(parquetDF, metadataDF).cache()
    val aggByAntenna = computeBytesSUMbyAntenna(antennaMetadataDF)
    val aggByApp = computeBytesSUMbyApp(antennaMetadataDF)
    val aggByUser = computeBytesSUMbyUser(antennaMetadataDF)
    val exceedquota = computeBytesExceedQuota(antennaMetadataDF)

    writeToJdbc(aggByAntenna, jdbcUri, aggJdbcTable, jdbcUser, jdbcPassword)
    writeToJdbc(aggByApp, jdbcUri, aggJdbcPercentTable, jdbcUser, jdbcPassword)
    writeToJdbc(aggByUser, jdbcUri, aggJdbcErrorTable, jdbcUser, jdbcPassword)
    writeToJdbc(exceedquota, jdbcUri, aggJdbcErrorTable, jdbcUser, jdbcPassword)


    spark.close()
  }

}
