package io.keepcoding.spark.exercise.batch
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.time.OffsetDateTime

object AntennaBatchJob extends BatchJob {

  override val spark: SparkSession = SparkSession
    .builder()
    .master(master = "local[20]")
    .appName(name = "Final Exercise SQL Streaming")
    .getOrCreate()

  import spark.implicits._

  override def readFromStorage(storagePath: String, filterDate: OffsetDateTime): DataFrame = {
    spark
      .read
      .format("parquet")
      .load(s"$storagePath/data")
      .filter(
        $"year" === filterDate.getYear &&
        $"month" === filterDate.getMonthValue &&
        $"day" === filterDate.getDayOfMonth &&
        $"hour" === filterDate.getHour
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

  override def computeDevicesCountByCoordinates(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .filter($"metric" === "devices_count")
      .select($"timestamp", $"location", $"value")
      .groupBy($"location", window($"timestamp", "1 hour").as("window"))
      .agg(
        avg($"value").as("avg_devices_count"),
        max($"value").as("max_devices_count"),
        min($"value").as("min_devices_count")
      )
      .select($"location", $"window.start".as("date"), $"avg_devices_count", $"max_devices_count", $"min_devices_count")
  }

  override def computeErrorAntennaByModelAndVersion(dataFrame: DataFrame): DataFrame = ???

  override def computePercentStatusByID(dataFrame: DataFrame): DataFrame = ???

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
      .save(s"$storageRootPath/historical")
  }

  def main(args: Array[String]): Unit = {
    val offsetDateTime = OffsetDateTime.parse("2022-12-08T11:00:00Z")
    val parquetDF = readFromStorage("/tmp/antenna_parquet/", offsetDateTime)


    val metadataDF = readAntennaMetadata("jdbc:postgresql://34.175.25.228:5432/postgres",
      "metadata",
      "postgres",
      "postgres")
    val antennaMetadataDF = enrichAntennaWithMetadata(parquetDF, metadataDF).cache()
    val aggByCoordinatesDF = computeDevicesCountByCoordinates(antennaMetadataDF)

    writeToJdbc(aggByCoordinatesDF, "jdbc:postgresql://34.175.25.228:5432/postgres",
      "antenna_1h_agg",
      "postgres",
      "postgres")

    writeToStorage(parquetDF, "/tmp/antenna_parquet/")

    spark.close()


  }

}
