package pl.training.hadoop;

import pl.training.hadoop.spark.Spark;

public class Application {

    public static void main(String[] args) {
        try (Spark spark = new Spark("local[*]", "WordsCount")) {
            spark.execute(new WorstMoviesTask());
        }
    }

}
