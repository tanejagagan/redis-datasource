import redis.clients.jedis.{Jedis, StreamEntryID}

object ReadTestData {


  def main(args: Array[String]): Unit = {
    val streamKey = args(0)
    val batchSize = args(1).toInt
    val iteration = args(2).toInt
    implicit val redisConfig : RedisConfig = new RedisConfig( RedisEndpoint(auth = "passwd"))
    readNoIterate(redisConfig, streamKey, batchSize, iteration)
    readData(redisConfig, streamKey, batchSize, iteration)
    System.exit(0)
  }

  def readData(redisConfig: RedisConfig, streamKey : String, batchSize : Int, iteration : Int): Unit = {
    val t3 = BenchmarkUtil.time(iteration , {
      val data = RedisStreamReader.getDataNG(redisConfig, streamKey, new StreamEntryID(0, 0),
        new StreamEntryID(Long.MaxValue, Long.MaxValue), batchSize)
      data.count(x => true)
    })
    println("Count is :" + t3._1)

  }

  def readNoIterate(redisConfig: RedisConfig, streamKey : String, batchSize : Int, iteration : Int): Unit = {
    val t3 = BenchmarkUtil.time(iteration, {
      ConnectionUtils.withConnection(redisConfig.connectionForKey(streamKey)) { conn: Jedis =>
        val client = conn.getClient
        client.xrange(streamKey, new StreamEntryID(0, 0),
          new StreamEntryID(Long.MaxValue, Long.MaxValue), batchSize)
        client.getObjectMultiBulkReply()
      }
    })
  }
}
