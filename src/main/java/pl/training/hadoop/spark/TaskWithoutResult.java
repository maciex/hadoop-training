package pl.training.hadoop.spark;

import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;

public interface TaskWithoutResult extends Serializable {

    void execute(JavaSparkContext sparkContext);

}
