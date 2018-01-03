package pl.training.hadoop;

import org.apache.spark.api.java.JavaSparkContext;
import pl.training.hadoop.spark.SparkExecutor;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class WordsCount {

    private static final String BASE_PATH = "hdfs:///user/maria_dev/%s";
    private static final String SEPARATOR = "\t";
    private static final String SOURCE_FILE = "u.data";
    private static final String DESTINATION_FILE = "wordsCount.txt";

    public static void main(String[] args) {
        new SparkExecutor().execute(WordsCount::wordsCountTask);
    }

    private static void wordsCountTask(JavaSparkContext sparkContext) {
        sparkContext.textFile(absolutePath(SOURCE_FILE))
                .flatMap(WordsCount::splitLine)
                .mapToPair(WordsCount::wordAsTuple)
                .reduceByKey(WordsCount::sum)
                .saveAsTextFile(absolutePath(DESTINATION_FILE));
    }

    private static Iterator<String> splitLine(String line) {
        return Arrays.asList(line.split(SEPARATOR)).iterator();
    }

    private static Tuple2<String, Integer> wordAsTuple(String word) {
        return new Tuple2<>(word, 1);
    }

    private static int sum(int value, int otherValue) {
        return value + otherValue;
    }

    private static String absolutePath(String file) {
        return String.format(BASE_PATH, file);
    }

}
