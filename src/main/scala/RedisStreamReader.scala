
import java.util
import java.util.{Collections, List => JList, Map => JMap}

import redis.clients.jedis.{BuilderFactory, Client, Jedis, StreamEntry, StreamEntryID}


/**
 * @author Gagan Taneja
 */


case class RedisStreamOffsetRange(start: StreamEntryID,
                                  end: StreamEntryID,
                                  batchSize: Int, streamKey: String)

object RedisStreamReader {

  def nextEntryFn( rev : Boolean) :  StreamEntryID => StreamEntryID = {
    if (rev) {
      streamEntry: StreamEntryID => {
        val nextEntry = if (streamEntry.getSequence == 0) {
          new StreamEntryID(streamEntry.getTime - 1, Long.MaxValue)
        } else {
          new StreamEntryID(streamEntry.getTime, streamEntry.getSequence - 1)
        }
        nextEntry
      }
    } else {
      streamEntry: StreamEntryID => {
        val nextEntry = new StreamEntryID(streamEntry.getTime, streamEntry.getSequence + 1)
        nextEntry
      }
    }
  }


  def getLastStreamEntryID(redisConfig: RedisConfig, streamKey: String): Option[StreamEntryID] = {
    ConnectionUtils.withConnection(redisConfig.connectionForKey(streamKey)) { conn: Jedis =>
      val revOrder = conn.xrevrange(streamKey,
        new StreamEntryID(Long.MaxValue, Long.MaxValue),
        new StreamEntryID(0, 0), 1)
      if (revOrder.size() == 1) {
        Some(revOrder.get(0).getID)
      } else {
        None
      }
    }
  }

  def batchFn( batchSize: Int, rev: Boolean = false ): ( Client, String, StreamEntryID, StreamEntryID) => JList[Any] = {
    val fn : ( Client, String, StreamEntryID, StreamEntryID) => JList[Any] = if(!rev) {
      val i : ( Client, String, StreamEntryID, StreamEntryID) => JList[Any] = {  ( client : Client, streamKey : String, start : StreamEntryID, end : StreamEntryID) =>
        client.xrange(streamKey, start, end, batchSize)
        client.getObjectMultiBulkReply().asInstanceOf[JList[Any]]
      }
      i
    } else {
      val i : ( Client, String, StreamEntryID, StreamEntryID) => JList[Any] =
      {  ( client : Client, streamKey : String, start : StreamEntryID, end : StreamEntryID) =>
        client.xrevrange(streamKey, start, end, batchSize)
        client.getObjectMultiBulkReply.asInstanceOf[JList[Any]]
      }
      i
    }
    fn
  }
  def getDataNG(redisConfig: RedisConfig, streamKey: String,
               start: StreamEntryID, end: StreamEntryID,
               batchSize: Int, rev: Boolean = false ): Iterator[Any] = {

    val fn = batchFn(batchSize, rev)
    val firstBatch: () => util.List[Any] = () => {
      ConnectionUtils.withConnection(redisConfig.connectionForKey(streamKey)) { conn: Jedis =>
        fn(conn.getClient, streamKey, start, end)
      }
    }

    val nextEntryF = nextEntryFn(rev)


    val nextBatch: Any => JList[Any] = { x =>
      Collections.EMPTY_LIST.asInstanceOf[JList[Any]]
    }
    /*val nextBatch: Any => util.List[Any] = streamEntry =>
      val nextStreamEntry = nextEntryF(streamEntry.getID)
      ConnectionUtils.withConnection(redisConfig.connectionForKey(streamKey)) { conn: Jedis =>
        fn(conn.getClient, streamKey, nextStreamEntry, end)
      }

     */
    new IteratorX(firstBatch, nextBatch, batchSize)
  }

  def getData(redisConfig: RedisConfig, streamKey: String,
              start: StreamEntryID, end: StreamEntryID,
              batchSize: Int, rev: Boolean = false ): Iterator[StreamEntry] = {

    val fetchFn: (Jedis, StreamEntryID) => util.List[StreamEntry] =
      if (rev) {
        (conn: Jedis, _start : StreamEntryID) => conn.xrevrange(streamKey, _start, end, batchSize)
      } else {
        (conn: Jedis, _start : StreamEntryID) => conn.xrange(streamKey, _start, end, batchSize)
      }
    val firstBatch: () => util.List[StreamEntry] = () => {
      ConnectionUtils.withConnection(redisConfig.connectionForKey(streamKey)) { conn: Jedis =>
        fetchFn(conn, start)
      }
    }
    val nextEntryF = nextEntryFn(rev)
    val nextBatch: StreamEntry => util.List[StreamEntry] = { streamEntry =>
      val nextStreamEntry = nextEntryF(streamEntry.getID)
      ConnectionUtils.withConnection(redisConfig.connectionForKey(streamKey)) { conn: Jedis =>
        fetchFn(conn, nextStreamEntry)
      }
    }
    new IteratorX(firstBatch, nextBatch, batchSize)
  }
}

class IteratorX[A](firstBatch: () => util.List[A],
                   nextBatch: A => java.util.List[A],
                   batchSize: Int) extends Iterator[A] {
  var currentBatch: java.util.List[A] = _
  var currentIterator: java.util.Iterator[A] = _
  var lastElem: A = _
  assert(batchSize > 0, "batch size should be greater than 0")
  var batchCount : Int = 0

  override def hasNext: Boolean = {
    if (currentBatch == null) {
      updateNextBatch(firstBatch())
    }
    if (currentIterator.hasNext()) {
      true;
    } else if (currentBatch.size() < batchSize) {
      false
    } else {
      updateNextBatch(nextBatch(lastElem))
      hasNext
    }
  }

  override def next(): A = {
    if (hasNext) {
      lastElem = currentIterator.next()
      lastElem
    } else {
      throw new NoSuchElementException()
    }
  }

  private def updateNextBatch(n: util.List[A]) {
    batchCount += 1
    currentBatch = n
    if (currentBatch == null) {
      currentBatch = Collections.emptyList()
    }
    currentIterator = currentBatch.iterator()
  }
}