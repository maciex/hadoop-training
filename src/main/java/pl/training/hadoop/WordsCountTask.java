package pl.training.hadoop;

import org.apache.spark.api.java.JavaSparkContext;
import pl.training.hadoop.spark.Hdfs;
import pl.training.hadoop.spark.TaskWithoutResult;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class WordsCountTask implements TaskWithoutResult {

    private static final String SEPARATOR = "\t";
    private static final String SOURCE_FILE = "u.data";
    private static final String DESTINATION_DIRECTORY = "WordsCount";
    private static final Hdfs hdfs = new Hdfs("hdfs:///user/maria_dev/%s");

    public void execute(JavaSparkContext sparkContext) {
        sparkContext.textFile(hdfs.absolutePath(SOURCE_FILE))
                .flatMap(this::splitLine)
                .mapToPair(this::wordToTuple)
                .reduceByKey(this::sum)
                .saveAsTextFile(hdfs.absolutePath(DESTINATION_DIRECTORY));
    }

    private Iterator<String> splitLine(String line) {
        return Arrays.asList(line.split(SEPARATOR)).iterator();
    }

    private Tuple2<String, Integer> wordToTuple(String word) {
        return new Tuple2<>(word, 1);
    }

    private int sum(int value, int otherValue) {
        return value + otherValue;
    }

}
