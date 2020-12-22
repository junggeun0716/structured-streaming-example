import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.scalatest.funsuite.AnyFunSuite

class WordCountTest extends AnyFunSuite {
  val spark = SparkSession
    .builder()
    .master("local")
    .appName("Word count example")
    .config("spark.sql.shuffle.partitions", "10")
    .config("spark.sql.default.parallelism", "10")
    .getOrCreate()

  test("The tumbling window counts words by window") {
    implicit val sqlContext = spark.sqlContext
    import spark.sqlContext.implicits._

    val inputStream = MemoryStream[WordCount]

    val query = WordCount.execute(spark, inputStream.toDF())

    inputStream.addData(
      Seq(
        WordCount(Timestamp.valueOf("2020-12-22 12:02:00"), "cat"),
        WordCount(Timestamp.valueOf("2020-12-22 12:02:00"), "dog"),
        WordCount(Timestamp.valueOf("2020-12-22 12:03:00"), "dog"),
        WordCount(Timestamp.valueOf("2020-12-22 12:03:00"), "dog"),
        WordCount(Timestamp.valueOf("2020-12-22 12:07:00"), "owl"),
        WordCount(Timestamp.valueOf("2020-12-22 12:07:00"), "cat"),
        WordCount(Timestamp.valueOf("2020-12-22 12:04:00"), "dog"),
        WordCount(Timestamp.valueOf("2020-12-22 12:13:00"), "owl")
      )
    )
    query.processAllAvailable()

    val result = spark.sql("select * from word_count")
    result.show(false)

    assert(result.count() == 4)
  }
}
