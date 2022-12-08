package io.keepcoding.spark.exercise.streaming

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object AntennaStreamingJob extends StreamingJob {
  override val spark: SparkSession = SparkSession
    .builder()
    .master(master = "local[20]")
    .appName(name = "Final Exercise SQL Streaming")
    .getOrCreate()

  import spark.implicits._

  override def readFromKafka(kafkaServer: String, topic: String): DataFrame = {
    spark
      .readStream
      .format(source = "kafka")
      .option("kafka.bootstrap.servers", kafkaServer)
      .option("subscribe", topic)
      .load()
}
  override def parserJsonData(dataFrame: DataFrame): DataFrame = {
    val jsonSchema = StructType(Seq(
        StructField("timestamp", TimestampType, nullable = false),
        StructField("id", StringType, nullable = false),
        StructField("metric", StringType, nullable = false),
        StructField("value", IntegerType, nullable = false)
      )
    )

    dataFrame
      .select(from_json($"value".cast(StringType), jsonSchema).as(alias = "json"))
      .select(cols= $"json.*")

  }


  override def readAntennaMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame = {
    spark
      .read
      .format(source="jdbc")
      .option("url", jdbcURI)
      .option("dbtable", jdbcTable)
      .option("user", user)
      .option("password", password)
      .load()

  }

  override def enrichAntennaWithMetadata(antennaDF: DataFrame, metadataDF: DataFrame): DataFrame = {
    antennaDF.as(alias="a")
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
      .withWatermark("timestamp", "10 seconds")
      .groupBy($"location", window($"timestamp", "30 seconds").as("window"))
      .agg(
        avg($"value").as("avg_devices_count"),
        max($"value").as("max_devices_count"),
        min($"value").as("min_devices_count")
      )
      .select($"location", $"window.start".as("date"), $"avg_devices_count", $"max_devices_count", $"min_devices_count")
  }

  override def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Future[Unit] = Future {
    dataFrame
      .writeStream
      .foreachBatch {
        (batch: DataFrame, _: Long) => {
          batch
            .write
            .mode(SaveMode.Append)
            .format("jdbc")
            .option("url", jdbcURI)
            .option("dbtable", jdbcTable)
            .option("user", user)
            .option("password", password)
            .save()
        }
      }
      .start()
      .awaitTermination()
  }

  override def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Future[Unit] = Future {
    dataFrame
      .select(
        $"id",$"timestamp", $"metric", $"value",
        year($"timestamp").as("year"),
        month($"timestamp").as("month"),
        dayofmonth($"timestamp").as("day"),
        hour($"timestamp").as("hour")
      )
      .writeStream
      .format("parquet")
      .option("path", s"$storageRootPath/data")
      .option("checkpointLocation", s"$storageRootPath/checkpoint")
      .partitionBy("year", "month", "day", "hour")
      .start
      .awaitTermination()

  }

  def main(args: Array[String]): Unit = {
    //run(args)
    val kafkaDF = readFromKafka("34.175.49.136:9092", topic = "antenna_telemetry")
    val parsedDF = parserJsonData(kafkaDF)

    val storageFuture = writeToStorage(parsedDF, "/tmp/antenna_parquet/")

    val metadaDF = readAntennaMetadata(
      "jdbc:postgresql://34.175.25.228:5432/postgres",
      "metadata",
      "postgres",
      "postgres"
    )

    val enrichDF = enrichAntennaWithMetadata(parsedDF,metadaDF)

    val countByLocation = computeDevicesCountByCoordinates(enrichDF)

    val jdbcFuture = writeToJdbc(countByLocation,
      "jdbc:postgresql://34.175.25.228:5432/postgres",
      "antenna_agg",
      "postgres",
      "postgres"
    )

    Await.result(
      Future.sequence(Seq(storageFuture, jdbcFuture)), Duration.Inf
    )

    countByLocation
      .writeStream
      .format(source = "console")

      .start()
      .awaitTermination()

  }

}
