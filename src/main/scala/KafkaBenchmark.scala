
import java.time.Duration
import java.util
import java.util.{Collections, Properties}

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition

object KafkaBenchmark {

  def main(args : Array[String]): Unit ={
    val topic = "test"
    val partition = "0"
    val numMsg = 3000000
    // producer("test", "1", numMsg)
    val res =BenchmarkUtil.time(6,
        consume(topic, partition , numMsg)
    )
    println("consumed :" + res)
  }

  def producer(topic : String, partition : String, numMsg: Long) {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("acks", "all")
    props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")

    val producer = new KafkaProducer[Array[Byte], Array[Byte]](props)
    var i = 0
    while (i < numMsg) {
      val msg = new ProducerRecord[Array[Byte], Array[Byte]](topic, Integer.toString(i).getBytes,
        Person.dataMaps2.head("name").getBytes)
      producer.send(msg)
      i += 1
      if(i%100 == 0){
        println("send 100")
      }
    }
    println("finished sending")
  }


  def consume(topic: String,
              partition: String,
              numMsg: Long) : Long = {

    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092");
    props.put("group.id", "test");
    props.put("enable.auto.commit", "false");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("max.poll.records", "100000")
    val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](props);
    val  partition0 = new TopicPartition(topic, partition.toInt);
    consumer.assign(util.Arrays.asList(partition0));

    var batch = 0
    //val records = consumer.poll(100);
    consumer.seek(partition0, 0L)
    var empty = false
    while (batch < numMsg && !empty) {
      val records = consumer.poll(100);
      val it = records.iterator()
      var empty = true
      while(it.hasNext){
        it.next()
        batch = batch +  1
        empty = false
      }
    }
    batch
  }
}

