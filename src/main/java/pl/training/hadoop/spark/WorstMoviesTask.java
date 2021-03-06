package pl.training.hadoop.spark;

import org.apache.spark.api.java.JavaSparkContext;
import pl.training.hadoop.model.Movie;
import pl.training.hadoop.Hdfs;
import scala.Tuple2;

import java.util.stream.StreamSupport;

public class WorstMoviesTask implements TaskWithoutResult {

    private static final String SEPARATOR = "\t";
    private static final String SOURCE_FILE = "u.data";
    private static final String DESTINATION_DIRECTORY = "WorstMovies";
    private static final Hdfs hdfs = new Hdfs("hdfs:///user/maria_dev/%s");

    public void execute(JavaSparkContext sparkContext) {
        sparkContext.textFile(hdfs.absolutePath(SOURCE_FILE))
                .map(this::lineToMovie)
                .groupBy(Movie::getId)
                .filter(this::popularMovie)
                .mapValues(this::moviesToAvgRating)
                .map(tuple -> new Movie(tuple._1, tuple._2))
                .sortBy(Movie::getRating, true, 0)
                .saveAsTextFile(DESTINATION_DIRECTORY);
    }

    private Movie lineToMovie(String line) {
        String[] fields = line.split(SEPARATOR);
        return new Movie(Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
    }

    private Boolean popularMovie(Tuple2<Long, Iterable<Movie>> tuple) {
        return tuple._2.spliterator().estimateSize() >= 10;
    }

    private Double moviesToAvgRating(Iterable<Movie> movies) {
        return StreamSupport.stream(movies.spliterator(), false)
                .mapToDouble(Movie::getRating)
                .average()
                .orElse(0);
    }

}
