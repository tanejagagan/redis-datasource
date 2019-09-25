import redis.clients.jedis.StreamEntryID

import collection.JavaConverters._

object InsertTestData {

  implicit val redisConfig : RedisConfig = new RedisConfig( RedisEndpoint(auth = "passwd"))

  def man(args : Array[String]): Unit ={
    val streamKey = args(0)
    val numEntries = args(1)
    insertData(streamKey, numEntries.toInt)
  }

  def insertData(streamKey : String, numEntries : Int) {
    ConnectionUtils.withConnection(streamKey) { conn =>
      (1 to numEntries).foreach { i =>
        conn.xadd(streamKey, StreamEntryID.NEW_ENTRY, Person.dataMaps2.head.asJava)
      }
    }
  }
}
