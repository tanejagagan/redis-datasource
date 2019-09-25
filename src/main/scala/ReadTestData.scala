import redis.clients.jedis.StreamEntryID

object ReadTestData {


  def main(args: Array[String]): Unit = {
    val streamKey = args(0)
    val batchSize = args(1).toInt
    val iteration = args(2).toInt
    implicit val redisConfig : RedisConfig = new RedisConfig( RedisEndpoint(auth = "passwd"))
    readData(redisConfig, streamKey, batchSize, iteration)
    System.exit(0)
  }

  def readData(redisConfig: RedisConfig, streamKey : String, batchSize : Int, iteration : Int): Unit = {
    val t3 = BenchmarkUtil.time(iteration , {
      val data = RedisStreamReader.getDataNG(redisConfig, streamKey, new StreamEntryID(0, 0),
        new StreamEntryID(Long.MaxValue, Long.MaxValue), batchSize)
      data.count(x => true)
    })
  }
}
