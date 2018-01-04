package pl.training.hadoop.spark;

import kafka.serializer.StringDecoder;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import pl.training.hadoop.Hdfs;
import pl.training.hadoop.model.Movie;
import pl.training.hadoop.model.Rating;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.singleton;

public class RatingsConsumerTask implements TaskWithoutResult {

    private static final int REFRESH_TIME_IN_MILLS = 5_000;
    private static String TOPIC_NAME = "messages";
    private static final String SOURCE_FILE = "u.item";
    private static final Hdfs hdfs = new Hdfs("hdfs:///user/maria_dev/%s");
    private static final String STATS_ACCUMULATOR = "stats";

    @Override
    public void execute(JavaSparkContext sparkContext) {
        JavaRDD<Tuple2<Long, String>> moviesMetadata = loadMoviesMetadata(sparkContext);
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkContext, new Duration(REFRESH_TIME_IN_MILLS));
        JavaPairInputDStream<String, String> stream = KafkaUtils.createDirectStream(streamingContext, String.class, String.class,
                StringDecoder.class, StringDecoder.class, prepareParameters(), singleton(TOPIC_NAME));

        Stats stats = new Stats();
        sparkContext.sc().register(stats, STATS_ACCUMULATOR);

        stream.foreachRDD(messages -> {
            System.out.println("######################################################################################");
            messages.map(this::messageToRating)
                    .collect()
                    .forEach(stats::add);

            stats.value().stream()
                    .sorted()
                    .limit(40)
                    .forEach(rating -> showStats(moviesMetadata, rating));
            });

        streamingContext.start();
        try {
            streamingContext.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void showStats(JavaRDD<Tuple2<Long, String>> moviesMetadata, Rating rating) {
        String title = moviesMetadata
            .filter(metadata -> metadata._1.equals(rating.getMovieId()))
            .first()._2;
        System.out.printf("%04d: Rating: %.2f (%03d) %s\n", rating.getMovieId(), rating.getAvg(), rating.getCount(), title);
    }

    private Rating messageToRating(Tuple2<String, String> message) {
        String[] fields = message._2.split("\\t");
        return  new Rating(Long.parseLong(fields[0]), 1, Long.parseLong(fields[2]));
    }

    private JavaRDD<Tuple2<Long, String>> loadMoviesMetadata(JavaSparkContext sparkContext) {
        return sparkContext.textFile(hdfs.absolutePath(SOURCE_FILE))
                .map(this::lineToMovieMetadata)
                .cache();
    }

    private Tuple2<Long, String> lineToMovieMetadata(String line) {
        String[] fields = line.split("\\|");
        return new Tuple2<>(Long.parseLong(fields[0]), fields[1]);
    }

    private Map<String, String> prepareParameters() {
        Map<String, String> parameters = new HashMap<>();
        parameters.put("metadata.broker.list", "sandbox-hdp:6667");
        return parameters;
    }

}
