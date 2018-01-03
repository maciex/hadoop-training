package pl.training.hadoop.spark;

import org.apache.spark.api.java.JavaSparkContext;

public interface TaskWithResult<T> {

    T execute(JavaSparkContext sparkContext);

}
