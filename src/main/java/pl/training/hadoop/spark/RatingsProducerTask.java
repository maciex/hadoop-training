package pl.training.hadoop.spark;

import org.apache.spark.api.java.JavaSparkContext;
import pl.training.hadoop.Hdfs;
import pl.training.hadoop.kafka.Producer;

public class RatingsProducerTask implements TaskWithoutResult {

    private static final String SOURCE_FILE = "u.data";
    private static final Hdfs hdfs = new Hdfs("hdfs:///user/maria_dev/%s");
    private static final int RATINGS_COUNT = 10_000;
    private static final int DELAY_IN_MILLS = 1_00;

    private Producer<String> producer = new Producer<>();

    @Override
    public void execute(JavaSparkContext sparkContext) {
        sparkContext.textFile(hdfs.absolutePath(SOURCE_FILE))
                .take(RATINGS_COUNT)
                .forEach(rating -> {
                    System.out.printf("Sending: %s\n", rating);
                    producer.send(rating);
                    delay();
                });
    }

    private void delay() {
        try {
            Thread.sleep(DELAY_IN_MILLS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
