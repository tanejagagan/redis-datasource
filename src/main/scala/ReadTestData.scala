import redis.clients.jedis.StreamEntryID

object ReadTestData {

  implicit val redisConfig : RedisConfig = new RedisConfig( RedisEndpoint(auth = "passwd"))
  def main(args: Array[String]): Unit = {
    val streamKey = args(0)
    val batchSize = args(1).toInt
    val rumRepeat = args(2).toInt
  }

  def readData(streamKey : String, batchSize : Int): Unit = {
    val t3 = BenchmarkUtil.time( 3, {
      val data = RedisStreamReader.getDataNG(redisConfig, streamKey, new StreamEntryID(0, 0),
        new StreamEntryID(Long.MaxValue, Long.MaxValue), batchSize)
      data.count(x => true)
    })
  }
}
