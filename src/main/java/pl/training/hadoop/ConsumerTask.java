package pl.training.hadoop;

import kafka.serializer.StringDecoder;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import pl.training.hadoop.spark.TaskWithoutResult;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.singleton;

public class ConsumerTask implements TaskWithoutResult {

    private static String TOPIC_NAME = "messages";

    @Override
    public void execute(JavaSparkContext sparkContext) {
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkContext, new Duration(1_000));
        JavaPairInputDStream<String, String> stream = KafkaUtils.createDirectStream(streamingContext, String.class, String.class,
                StringDecoder.class, StringDecoder.class, prepareParameters(), singleton(TOPIC_NAME));

        stream.foreachRDD(rdd -> {
            rdd.foreach(record -> System.out.println(record._2));
        });

        streamingContext.start();
        try {
            streamingContext.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private Map<String, String> prepareParameters() {
        Map<String, String> parameters = new HashMap<>();
        parameters.put("metadata.broker.list", "sandbox-hdp:6667");
        return parameters;
    }

}
