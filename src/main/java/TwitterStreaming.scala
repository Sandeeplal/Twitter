import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TwitterStreaming {


  def main(args: Array[String]) {

    val spark = new SparkSession.Builder().appName("TwitterStreaming").master("local[*]").getOrCreate()

    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
    val filters = args.takeRight(args.length - 4)
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    val ssc = new StreamingContext(sc, Seconds(10))
    val stream = TwitterUtils.createStream(ssc, None, filters)

    // Split the stream on space and extract hashtags
    val hashtags = stream.flatMap(status => status.getText().split(" ").filter(_.startsWith("#")))
    stream.map(l => l.getText)
      .saveAsTextFiles("C:\\Users\\sandeep\\IdeaProjects\\Twitter\\Data Captured\\")

    // Get the top hashtags over the previous 60 sec window
    val topCounts60 = hashtags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
      .map { case (topic, count) => (count, topic) }
      .transform(_.sortByKey(false))

    // Print popular hashtags

    topCounts60.print(5)
    topCounts60.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nPopular topics in last 60 seconds (%s total):".format(rdd.count()))
      topList.foreach { case (count, tag) => println("%s (%s tweets)".format(tag, count)) }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}