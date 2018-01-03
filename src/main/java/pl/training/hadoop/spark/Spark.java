package pl.training.hadoop.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class Spark implements AutoCloseable {

    private JavaSparkContext sparkContext;

    public Spark(String master, String appName) {
        SparkConf sparkConfig = new SparkConf()
                .setMaster(master)
                .setAppName(appName);
        sparkContext = new JavaSparkContext(sparkConfig);
    }

    public <T> T execute(Task<T> sparkTask) {
        return sparkTask.execute(sparkContext);
    }

    public void execute(TaskWithoutResult sparkTask) {
        sparkTask.execute(sparkContext);
    }

    @Override
    public void close() {
        sparkContext.close();
    }

}
