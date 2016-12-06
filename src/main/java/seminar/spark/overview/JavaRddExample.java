package seminar.spark.overview;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;

public class JavaRddExample {

    public static final int PARTITION_SIZE = 200;

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("JavaRddExample")
                .getOrCreate();

        String path = "/tmp/data/alice-in-wonderland.txt";
        JavaSparkContext sparkContext = new JavaSparkContext(spark.sparkContext());
        JavaRDD<String> text = sparkContext.textFile(path);

        //Lets count number of non empty lines
        long numberOfNonEmptyLines = text.filter(line -> !line.isEmpty()).count();
        System.out.println("There are " + numberOfNonEmptyLines + " non empty lines");

        //Lets count number of empty lines
        long numberOfEmptyLines = text.filter(line -> line.isEmpty()).count();
        System.out.println("There are " + numberOfEmptyLines + " empty lines");

        //What is the most frequent 5 letter word
        Tuple2<String, Integer> result = text.flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .filter(word -> word.length() == 5)
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((sum1, sum2) -> sum1 + sum2)
                .rdd()
                .toJavaRDD()
                .sortBy(tuple -> tuple._2(), false, PARTITION_SIZE)
                .first();

        System.out.println("The most frequent 5 letter word is '" + result._1() +
                "', number of occurrences is " + result._2());
    }
}
