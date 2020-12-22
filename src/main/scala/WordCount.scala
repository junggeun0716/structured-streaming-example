import java.sql.Timestamp

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery

case class WordCount(timestamp: Timestamp, word: String)
object WordCount {
  def execute(spark: SparkSession, stream: DataFrame): StreamingQuery = {
    import spark.implicits._

    val windowedCounts = stream
      .withWatermark("timestamp", "10 minutes")
      .groupBy(
        window($"timestamp", "10 minutes"),
        $"word")
      .count()

    windowedCounts
      .writeStream
      .queryName("word_count")
      .format("memory")
      .outputMode("update")
      .start()
  }
}
