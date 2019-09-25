import java.util.UUID

import TestUtils._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.reflect.Manifest


object TestUtils {

  def generateTableName(prefix: String): String = {
    // generate random table, so we can run test multiple times and not append/overwrite data
    prefix + UUID.randomUUID().toString.replace("-", "")
  }

  def generateRandomKey(): String = {
    UUID.randomUUID().toString.replace("-", "")
  }

  /**
   * A wrapper of Assertions.intercept() that suppresses spark errors in the logs.
   * It makes easier to analyse unit tests output.
   */
  def interceptSparkErr[T <: AnyRef](f: => Any)(implicit manifest: Manifest[T]): T = {
    // turn off spark logger
    val logger = org.apache.log4j.Logger.getLogger("org")
    val levelBefore = logger.getLevel
    logger.setLevel(org.apache.log4j.Level.OFF)

    // delegate interception
    val interceptRes = Assertions.intercept(f)

    // revert logger
    logger.setLevel(levelBefore)

    interceptRes
  }


}

/**
 * @author The Viet Nguyen
 */
case class Person(name: String, age: Int, address: String, salary: Double)

object Person {

  val TableNamePrefix = "person"
  val KeyName = "name"

  val data = Seq(
    Person("John", 30, "60 Wall Street", 150.5),
    Person("Peter", 35, "110 Wall Street", 200.3)
  )

  val dataMaps = Seq(
    Map("name" -> "John", "age" -> "30", "address" -> "60 Wall Street", "salary" -> "150.5"),
    Map("name" -> "Peter", "age" -> "35", "address" -> "110 Wall Street", "salary" -> "200.3")
  )

  val dataMaps2 = Seq(
    Map("name" ->
      "John dsfasdfas sdfasfasfasdfasdfasdfasdfasdfasdfasdfsadfskadfkasjdfkjsaldfasldgjfldafgkjhsjhfjwehrklakfjslkajflksajlfjas"),
    Map("name" -> "Peter", "age" -> "35", "address" -> "110 Wall Street", "salary" -> "200.3")
  )

  val schema = StructType(Array(
    StructField("name", StringType),
    StructField("age", IntegerType),
    StructField("address", StringType),
    StructField("salary", DoubleType)
  ))

  val fullSchema = StructType(schema.fields :+ StructField("_id", StringType))

  def df(spark: SparkSession): DataFrame = spark.createDataFrame(data)

  def generatePersonTableName(): String = generateTableName(TableNamePrefix)

  def generatePersonStreamKey(): String = generatePersonTableName()

}
