package seminar.spark.overview;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import java.util.List;

public class JavaSparkSessionExample {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("JavaSparkSessionExample")
                .getOrCreate();

        String path = "/tmp/data/alice-in-wonderland.txt";
        Dataset<String> text = spark.read().textFile(path);

        //Lets look what's inside
        List<String> beginning = text.takeAsList(20);
        beginning.forEach(line -> System.out.println(line));
    }
}
