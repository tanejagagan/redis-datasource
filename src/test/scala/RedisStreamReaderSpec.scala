import java.util.Collections

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import redis.clients.jedis.StreamEntryID

import collection.JavaConverters._

class RedisStreamReaderSpec extends FunSuite with BeforeAndAfterAll {

  implicit val redisConfig : RedisConfig = new RedisConfig( RedisEndpoint(auth = "passwd"))

  test("insert data to stream"){
    val testStream = "test2"
    //insertData(testStream, 1000000)
    val latestEntry = RedisStreamReader.getLastStreamEntryID(redisConfig, testStream)

    /*
    val t1 = time {
      val data = RedisStreamReader.getData(redisConfig, testStream, new StreamEntryID(0, 0),
        latestEntry.get, 100000)
      data.count(x => true)
    }

    println(s"count $t1")
    val t2 = time {
      val revData = RedisStreamReader.getData(redisConfig, testStream,
        latestEntry.get, new StreamEntryID(0, 0), 100000, true)
      revData.count(x=>true)
    }

     */

    val t3 = BenchmarkUtil.time (1 ,{
      val data = RedisStreamReader.getDataNG(redisConfig, testStream, new StreamEntryID(0, 0),
        latestEntry.get, 1000000)
      data.count(x => true)
    })
    println(s"count $t3")
  }

  def insertData(streamKey : String, numEntries : Int) {
    ConnectionUtils.withConnection(streamKey) { conn =>
      (1 to numEntries).foreach { i =>
        conn.xadd(streamKey, StreamEntryID.NEW_ENTRY, Person.dataMaps2.head.asJava)
      }
    }
  }


}
