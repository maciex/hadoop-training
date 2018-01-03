package pl.training.hadoop.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkExecutor {

    public void execute(Task sparkTask) {
        SparkConf sparkConfig = new SparkConf()
                .setMaster("local[*]")
                .setAppName("Words count");

        try (JavaSparkContext sparkContext = new JavaSparkContext(sparkConfig)) {
            sparkTask.execute(sparkContext);
        }
    }

}
