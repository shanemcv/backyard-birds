package smcveigh

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import com.fasterxml.jackson.databind.{ DeserializationFeature, ObjectMapper }
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes

object StreamBirdObservations {
  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
  val hbaseConf: Configuration = HBaseConfiguration.create()


  val hbaseConnection = ConnectionFactory.createConnection(hbaseConf)
  val table = hbaseConnection.getTable(TableName.valueOf("smcveigh_birds_by_state_speed"))
  
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println(s"""
        |Usage: StreamBirdObservations <brokers>
        |  <brokers> is a list of one or more Kafka brokers
        | 
        """.stripMargin)
      System.exit(1)
    }

    val Array(brokers) = args

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("SmcveighBirdObservations")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create direct kafka stream with brokers and topics
    val topicsSet = Set("smcveigh_bird_observations")
    // Create direct kafka stream with brokers and topics
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "smcveigh_bird_observations_consumer",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean),
      "security.protocol" -> "SASL_SSL",
      "sasl.mechanism" -> "SCRAM-SHA-512",
      "sasl.jaas.config" -> ("org.apache.kafka.common.security.scram.ScramLoginModule required " + "username=\"mpcs53014-2025\" password=\"replacewithpassword\";")

    )
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc, PreferConsistent,
      Subscribe[String, String](topicsSet, kafkaParams)
    )

    // Get the lines, split them into words, count the words and print
    val serializedRecords = stream.map(_.value);
    val observations = serializedRecords.map(rec => mapper.readValue(rec, classOf[BirdObservation]))

    // How to write to an HBase table
    val batchStats = observations.map(obs => {
      val rowKey = obs.stateProvince + "_" + obs.species
      val get = new org.apache.hadoop.hbase.client.Get(Bytes.toBytes(rowKey))
      val result = table.get(get)

      // current sightings count, default 0 if row doesn't exist
      //val currentCount = if (!result.isEmpty) {
      //  Bytes.toInt(result.getValue(Bytes.toBytes("stats"), Bytes.toBytes("sightings")))
      //} else 0
      val currentCount = {
        val bytes = result.getValue(Bytes.toBytes("stats"), Bytes.toBytes("sightings"))
        if (bytes != null && bytes.length == 4) {
          Bytes.toInt(bytes)
        } else if (bytes != null) {
          new String(bytes).toInt
        } else 0
      }

      // increment count
      val put = new Put(Bytes.toBytes(rowKey))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("stateProvince"), Bytes.toBytes(obs.stateProvince))
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("species"), Bytes.toBytes(obs.species))
      put.addColumn(Bytes.toBytes("stats"), Bytes.toBytes("sightings"), Bytes.toBytes(currentCount + 1))

      table.put(put)
    })
    batchStats.print()
    
    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }

}
