package pl.training.hadoop;

import pl.training.hadoop.spark.RatingsProducerTask;
import pl.training.hadoop.spark.Spark;

import java.io.IOException;

public class Application {

   public static void main(String[] args) throws IOException {
        try (Spark spark = new Spark("local[*]", "Consumer")) {
            spark.execute(new RatingsProducerTask());
        }
    }

}
