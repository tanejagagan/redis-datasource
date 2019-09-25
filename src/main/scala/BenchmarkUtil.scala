object BenchmarkUtil {
  def time[T]( iteration : Int, op : => T) : (T, Long) = {
    var i = 0
    var res : T = null.asInstanceOf[T]
    var minTime = 0L
    var maxTime = 0L
    var totalTime  = 0L
    while (i < iteration) {
      val start = System.nanoTime()
      res = op
      val end = System.nanoTime()
      val t = end - start
      if(t < minTime)
        minTime = t
      if(t > maxTime)
        maxTime = t
      totalTime += t
      i +=1
      print(s"Finished iteration $i")
    }
    println(s" $res =>Average time taken ${totalTime/iteration} nsec = ${totalTime/(iteration * 1000000)} mlsec")
    (res, totalTime/iteration)
  }
}
