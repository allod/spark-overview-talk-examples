package seminar.spark.overview

import org.apache.spark.sql._

object ScalaDatasetExample {

  case class Person(firstName: String, lastName: String, companyName: String, zip: Int, email: String)

  case class ZipCode(zip: Int, city: String, county: String, state: String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
//      .master("local[*]")
      .appName("ScalaDatasetExample")
      .getOrCreate()

    import spark.implicits._

    val zipTablePath = "/tmp/data/zip.csv"
    val personTablePath = "/tmp/data/persons.parquet"

    val zipCodeDS: Dataset[ZipCode] = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(zipTablePath)
      .as[ZipCode]

    //Print zipCode schema
    zipCodeDS.printSchema

    //Print first 20 rows from zipCode dataset
    zipCodeDS.show

    val personDS: Dataset[Person] = spark.read
      .parquet(personTablePath)
      .as[Person]

    //Print person schema
    personDS.printSchema

    //Print first 20 rows from person dataset
    personDS.show

    val joinCondition = personDS("zip") === zipCodeDS("zip")
    val joined = personDS.join(zipCodeDS, joinCondition)
    val personInfo = joined.select($"firstName", $"lastName", $"city")

    //Print first 20 rows from joined dataset
    personInfo.show

    //Save to json file
    personInfo.write.json("/tmp/data/persons.json")
  }
}
