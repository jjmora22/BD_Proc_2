package io.keepcoding.spark.exercise.batch
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.time.OffsetDateTime

object PracticaBatchJob extends BatchJob {

  override val spark: SparkSession = SparkSession
    .builder()
    .master(master = "local[20]")
    .appName(name = "Final Practice SQL Streaming")
    .getOrCreate()

  import spark.implicits._

  override def readFromStorage(storagePath: String, filterDate: OffsetDateTime): DataFrame = {
    spark
      .read
      .format("parquet")
      .load(s"$storagePath/data")
      .filter(
          year($"timestamp").as("year") === filterDate.getYear &&
          month($"timestamp").as("month") === filterDate.getMonthValue &&
          dayofmonth($"timestamp").as("day") === filterDate.getDayOfMonth &&
          hour($"timestamp").as("hour") === filterDate.getHour
      )
  }


  override def readAntennaMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame = {
    spark
      .read
      .format(source = "jdbc")
      .option("url", jdbcURI)
      .option("dbtable", jdbcTable)
      .option("user", user)
      .option("password", password)
      .load()

  }

  override def enrichAntennaWithMetadata(antennaDF: DataFrame, metadataDF: DataFrame): DataFrame = {
    antennaDF.as(alias = "a")
      .join(
        metadataDF.as("b"),
        $"a.id" === $"b.id"
      )
      .drop($"b.id")
  }

  override def computeBytesSUMbyAntenna(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"antenna_id", $"timestamp", $"bytes")
      .groupBy($"antenna_id", window($"timestamp", "1 hour")) //1 hour
      .agg(
        sum($"bytes").as("value"),
      )
      .withColumn("type", lit("antenna_bytes_total"))
      .select($"window.start".as("timestamp"), $"antenna_id".as("id"), $"value", $"type")

  }

  override def computeBytesSUMbyApp(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"app", $"timestamp", $"bytes")
      .groupBy($"app", window($"timestamp", "1 hour")) //1 hour
      .agg(
        sum($"bytes").as("value"),
      )
      .withColumn("type", lit("app_bytes_total"))
      .select($"window.start".as("timestamp"), $"app".as("id"), $"value", $"type")

  }

  override def computeBytesSUMbyUser(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"email", $"bytes", $"quota", $"timestamp")
      .groupBy($"email", window($"timestamp", "1 hour"), $"quota") //1 hour
      .agg(
        sum($"bytes").as("usage"),
      )
      .select($"email", $"usage", $"quota", $"window.start".as("timestamp"))

  }

  override def computeBytesExceedQuota(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"email", $"bytes", $"quota", $"timestamp")
      .groupBy($"email", window($"timestamp", "1 hour"), $"quota") //1 hour
      .agg(
        sum($"bytes").as("usage"),
      )
      .filter(
        $"quota" < $"usage"
      )
      .withColumn("Exceeded", lit("has_exceeded"))
      .select($"email", $"usage", $"quota", $"window.start".as("timestamp"), $"Exceeded")

  }

  override def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Unit = {
    dataFrame
      .write
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("url", jdbcURI)
      .option("dbtable", jdbcTable)
      .option("user", user)
      .option("password", password)
      .save()

  }

  override def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Unit = {
    dataFrame
      .write
      .format("parquet")
      .partitionBy("year", "month", "day", "hour")
      .mode(SaveMode.Overwrite)
      .save(s"$storageRootPath/historical_practica")
  }

  def main(args: Array[String]): Unit = {
    val offsetDateTime = OffsetDateTime.parse("2022-12-08T14:25Z")
    val parquetDF = readFromStorage("/tmp/parsedDF_parquet/", offsetDateTime)

    val metadataDF = readAntennaMetadata("jdbc:postgresql://34.175.25.228:5432/postgres",
    "user_metadata",
    "postgres",
    "postgres")

    val antennaMetadataDF = enrichAntennaWithMetadata(parquetDF, metadataDF).cache()
    val aggByAntenna = computeBytesSUMbyAntenna(antennaMetadataDF)
    writeToJdbc(aggByAntenna, "jdbc:postgresql://34.175.25.228:5432/postgres",
      "bytes_hourly",
      "postgres",
      "postgres")

    val aggByApp = computeBytesSUMbyApp(antennaMetadataDF)
    writeToJdbc(aggByApp, "jdbc:postgresql://34.175.25.228:5432/postgres",
      "bytes_hourly",
      "postgres",
      "postgres")

    val aggByUser = computeBytesSUMbyUser(antennaMetadataDF)
    writeToJdbc(aggByUser, "jdbc:postgresql://34.175.25.228:5432/postgres",
      "user_quota_limit",
      "postgres",
      "postgres")

    val exceedquota = computeBytesExceedQuota(antennaMetadataDF)
    writeToJdbc(exceedquota, "jdbc:postgresql://34.175.25.228:5432/postgres",
      "exceeded",
      "postgres",
      "postgres")



  }

}

