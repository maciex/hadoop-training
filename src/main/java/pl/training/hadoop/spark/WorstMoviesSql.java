package pl.training.hadoop.spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import pl.training.hadoop.Hdfs;
import pl.training.hadoop.model.Metadata;
import pl.training.hadoop.model.Movie;

import static org.apache.spark.sql.functions.*;

public class WorstMoviesSql {

    private static final String MOVIES_SEPARATOR = "\t";
    private static final String MOVIES_SOURCE_FILE = "u.data";
    private static final String METADATA_SEPARATOR = "\\|";
    private static final String METADATA_SOURCE_FILE = "u.item";
    private static final Hdfs hdfs = new Hdfs("hdfs:///user/maria_dev/%s");

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("WorstMovies")
                .getOrCreate();

        JavaRDD<Movie> movies = sparkSession.read()
                .textFile(hdfs.absolutePath(MOVIES_SOURCE_FILE))
                .javaRDD()
                .map(WorstMoviesSql::lineToMovie);

        JavaRDD<Metadata> metadata = sparkSession.read()
                .textFile(hdfs.absolutePath(METADATA_SOURCE_FILE))
                .javaRDD()
                .map(WorstMoviesSql::lineToMetadata);

        RelationalGroupedDataset moviesDataset = sparkSession
                .createDataFrame(movies, Movie.class)
                .groupBy("id");

        Dataset<Row> metadataDataset = sparkSession
                .createDataFrame(metadata, Metadata.class);

        moviesDataset.avg("rating")
                .join(moviesDataset.count(), "id")
                .filter("count >= 10")
                .join(metadataDataset, "id")
                .orderBy(asc("avg(rating)"), desc("count"))
                .takeAsList(10)
                .forEach(System.out::println);

        sparkSession.close();
    }

    private static Movie lineToMovie(String line) {
        String[] fields = line.split(MOVIES_SEPARATOR);
        return new Movie(Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
    }

    private static Metadata lineToMetadata(String line) {
        String[] fields = line.split(METADATA_SEPARATOR);
        return new Metadata(Long.parseLong(fields[0]), fields[1]);
    }

}
