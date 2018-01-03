package pl.training.hadoop;

import org.apache.spark.api.java.JavaSparkContext;
import pl.training.hadoop.spark.TaskWithoutResult;

public class ConsumerTask implements TaskWithoutResult {

    @Override
    public void execute(JavaSparkContext sparkContext) {

    }

}
