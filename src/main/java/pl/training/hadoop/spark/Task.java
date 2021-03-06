package pl.training.hadoop.spark;

import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;

public interface Task<T> extends Serializable {

    T execute(JavaSparkContext sparkContext);

}
