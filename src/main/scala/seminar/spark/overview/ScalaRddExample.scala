package seminar.spark.overview

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ScalaRddExample {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("ScalaSparkSessionExample")
      .getOrCreate()

    val path = "/tmp/data/alice-in-wonderland.txt"
    val text: RDD[String] = spark.sparkContext.textFile(path)

    //Lets count number of non empty lines
    val numberOfNonEmptyLines = text.filter(line => line.nonEmpty).count()
    println(s"There are $numberOfNonEmptyLines non empty lines")

    //Lets count number of empty lines
    val numberOfEmptyLines = text.filter(line => line.isEmpty).count()
    println(s"There are $numberOfEmptyLines empty lines")

    //What is the most frequent 5 letter word
    val (mostFrequentWord, wordCount) = text
      .flatMap(line => line.split(" "))
      .filter(word => word.length == 5)
      .map(word => (word, 1))
      .reduceByKey((sum1, sum2) => sum1 + sum2)
      .sortBy({ case (word, count) => count }, ascending = false)
      .first()

    println(s"The most frequent 5 letter word is '$mostFrequentWord', number of occurrences is $wordCount")
  }
}
