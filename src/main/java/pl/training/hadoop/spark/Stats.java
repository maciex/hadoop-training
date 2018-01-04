package pl.training.hadoop.spark;

import org.apache.spark.util.AccumulatorV2;
import pl.training.hadoop.model.Rating;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Stats extends AccumulatorV2<Rating, List<Rating>> {

    private Map<Long, Rating> ratings = new HashMap<>();

    @Override
    public boolean isZero() {
        return ratings.isEmpty();
    }

    @Override
    public AccumulatorV2<Rating, List<Rating>> copy() {
        throw new NotImplementedException();
    }

    @Override
    public void reset() {
        ratings.clear();
    }

    @Override
    public synchronized void add(Rating newRating) {
        Long movieId = newRating.getMovieId();
        Rating rating = ratings.get(movieId);
        if (rating != null) {
            rating.addRating(newRating);
        } else {
            ratings.put(movieId, newRating);
        }
    }

    @Override
    public synchronized  void merge(AccumulatorV2<Rating, List<Rating>> accumulatorV2) {
        accumulatorV2.value().forEach(this::add);
    }

    @Override
    public List<Rating> value() {
        return new ArrayList<>(ratings.values());
    }

}
