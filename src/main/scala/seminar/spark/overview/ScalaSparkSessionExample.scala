package seminar.spark.overview

import org.apache.spark.sql.SparkSession

object ScalaSparkSessionExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("ScalaSparkSessionExample")
      .getOrCreate()

    val text = spark.read.textFile("/tmp/data/alice-in-wonderland.txt")

    //Lets look what's inside
    val beginning = text.take(20)
    beginning.foreach(line => println(line))
  }
}
