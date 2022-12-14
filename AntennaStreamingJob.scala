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
    .appName(name = "Final Practice SQL Streaming")
    .getOrCreate()

  import spark.implicits._

  override def readFromKafka(kafkaServer: String, topic: String): DataFrame = {
    spark
      .readStream
      .format(source= "kafka")
      .option("kafka.bootstrap.servers", kafkaServer)
      .option("subscribe", topic)
      .load()
  }

  override def parserJsonData(dataFrame: DataFrame): DataFrame = {
    val jsonSchema = StructType(Seq(

       //Estructura tabla práctica -->

        StructField("timestamp", TimestampType, nullable = false), //practica
        StructField("id", StringType, nullable = false), //practica
        StructField("antenna_id", StringType, nullable = false), //practica
        StructField("bytes", LongType, nullable = false), //practica
        StructField("app", StringType, nullable = false), //practica

      )
    )
    dataFrame
      .select(from_json($"value".cast(StringType), jsonSchema).as(alias="json"))
      .select(cols = $"json.*")

  }

  /*override def readAntennaMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame = {
    spark
      .read
      .format(source = "jdbc")
      .option("url", jdbcURI)
      .option("dbtable", jdbcTable)
      .option("user", user)
      .option("password", password)
      .load()

  }
*/
  //override def enrichAntennaWithMetadata(antennaDF: DataFrame, metadataDF: DataFrame): DataFrame = {
  //  antennaDF.as(alias = "a")
  //  .join(
  //      metadataDF.as(alias = "b"),
  //      joinExprs = $"a.id" === $"b.id" // -- Práctica
        //joinExprs = $"a.id" === $"b.id" // -- Ejercicio final
  //    )
  //    .drop (col = $"b.id")
 //}



  //AQUI INICIAN LOS COMPUTE DE LA PRÁCTICA

  override def computeBytesSUMbyAntenna(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"antenna_id", $"id", $"app", $"timestamp", $"bytes")
      .withWatermark("timestamp", "10 seconds") // 1 minute
      .groupBy($"antenna_id", window($"timestamp", "30 seconds"))//1 hour
      .agg(
        sum($"bytes").as("value"),
      )
      .withColumn("type", lit("antenna_bytes_total"))
      .select($"window.start".as("timestamp"), $"antenna_id".as("id"), $"value", $"type")

  }

  override def computeBytesSUMbyUser(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"antenna_id", $"id", $"app", $"timestamp", $"bytes")
      .withWatermark("timestamp", "10 seconds") // 1 minute
      .groupBy($"id", window($"timestamp", "30 seconds"))//1 hour
      .agg(sum($"bytes").as("value"),
      )
      .withColumn("type", lit("user_bytes_total"))
      .select($"window.start".as("timestamp"), $"id".as("id"), $"value", $"type")

  }

  override def computeBytesSUMbyAPP(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"antenna_id", $"id", $"app", $"timestamp", $"bytes")
      .withWatermark("timestamp", "10 seconds") // 1 minute
      .groupBy($"app", window($"timestamp", "30 seconds"))//1 hour
      .agg(sum($"bytes").as("value"),
      )
      .withColumn("type", lit("app_bytes_total"))
      .select($"window.start".as("timestamp"), $"app".as("id"), $"value", $"type")
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
        $"timestamp", $"id", $"antenna_id", $"bytes", $"app"
      )
      .writeStream
      .format("parquet")
      .option("path", s"$storageRootPath/data")
      .option("checkpointLocation", s"$storageRootPath/checkpoint")
      .partitionBy("timestamp")
      .start
      .awaitTermination()

  }


  def main(args: Array[String]): Unit = {

    val kafkaDF = readFromKafka(kafkaServer="34.175.49.136:9092", topic="devices")// Aquí hay que cambiar el tópico para la práctica
    val parsedDF = parserJsonData(kafkaDF)

    val storageFuture = writeToStorage(parsedDF, "/tmp/parsedDF_parquet/")

  //  val storageFuture = writeToStorage(parsedDF, storageRootPath = "../bdprocessing/")
    //val metadaDF = readAntennaMetadata(
    //  jdbcURI = "jdbc:postgresql://34.175.25.228:5432/postgres",
      //jdbcTable = "metadata",         //ejercicio final
    //  jdbcTable = "user_metadata", //practica
    //  user = "postgres",
    //  password = "postgres"
    //)

   // val enrichDF = enrichAntennaWithMetadata(parsedDF, metadaDF)
    // val countByLocation = computeDevicesCountByCoordinates(parsedDF) //-- Ejercicio Final
    val countByLocation = computeBytesSUMbyAntenna(parsedDF) //-- Práctica
    val jdbcFuture = writeToJdbc(countByLocation, jdbcURI = "jdbc:postgresql://34.175.25.228:5432/postgres", jdbcTable = "bytes", user = "postgres", password = "postgres")

    val countByUser = computeBytesSUMbyUser(parsedDF)
    val jdbcFuture1 = writeToJdbc(countByUser, jdbcURI = "jdbc:postgresql://34.175.25.228:5432/postgres", jdbcTable="bytes", user="postgres", password="postgres")

    val countByApp = computeBytesSUMbyAPP(parsedDF)
    val jdbcFuture2 = writeToJdbc(countByApp, jdbcURI = "jdbc:postgresql://34.175.25.228:5432/postgres", jdbcTable = "bytes", user = "postgres", password = "postgres")

   // val resultantDF = countByLocation.union(countByUser)
   // val resultantDF2 = resultantDF.union(countByApp)

    //val countByApp = computeBytesSUMbyApp(enrichDF) // -- Práctica
    //val jdbcFuture2 = writeToJdbc(countByApp, jdbcURI = "jdbc:postgresql://34.175.25.228:5432/postgres", jdbcTable = "antenna_agg", user = "postgres", password = "postgres")

    Await.result(
      Future.sequence(Seq(storageFuture, jdbcFuture, jdbcFuture1, jdbcFuture2)), Duration.Inf
    )

   // spark.close()



    countByUser
      .writeStream
      .format(source="console")
      .start()
      .awaitTermination()

    }
}
