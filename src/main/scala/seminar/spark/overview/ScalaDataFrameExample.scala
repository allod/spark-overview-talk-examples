package seminar.spark.overview

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object ScalaDataFrameExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("ScalaDataFrameExample")
      .getOrCreate()

    import spark.implicits._

    val personsJsonTablePath = "/tmp/data/persons.json"
    val personDF: Dataset[Row] = spark.read.json(personsJsonTablePath)

    //Do some sql queries with programmatic API
    val resultDF = personDF.select($"firstName").where($"city" === "Chicago")

    //Show persons from Chicago
    resultDF.show

    //Create temp table
    personDF.createOrReplaceTempView("persons")

    //Query persons temp table
    spark.sql("select * from persons order by city").show
  }
}
