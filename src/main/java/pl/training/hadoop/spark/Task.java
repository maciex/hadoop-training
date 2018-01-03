package pl.training.hadoop.spark;

import org.apache.spark.api.java.JavaSparkContext;

public interface Task {

    void execute(JavaSparkContext sparkContext);

}
