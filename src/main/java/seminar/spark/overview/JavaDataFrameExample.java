package seminar.spark.overview;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JavaDataFrameExample {
    public static void main(String[] args) {
        String personsJsonTablePath = "/tmp/data/persons.json";

        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("JavaDataFrameExample")
                .getOrCreate();

        Dataset<Row> personDF = spark.read().json(personsJsonTablePath);

        //Do some sql queries with programmatic API
        Dataset<Row> resultDF = personDF.select("firstName").where(personDF.col("city").equalTo("Chicago"));

        //Show persons from Chicago
        resultDF.show();

        //Create temp table
        personDF.createOrReplaceTempView("persons");

        //Query persons temp table
        spark.sql("select * from persons order by city").show();
    }
}
