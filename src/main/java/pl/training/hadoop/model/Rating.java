package pl.training.hadoop.model;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.io.Serializable;

@RequiredArgsConstructor
@NoArgsConstructor
@Data
public class Rating implements Serializable, Comparable<Rating> {

    @NonNull
    private Long movieId;
    @NonNull
    private long count;
    @NonNull
    private long ratingsSum;

    public Double getAvg() {
        return (double) ratingsSum / (double) count;
    }

    public void addRating(Rating rating) {
        ratingsSum += rating.getRatingsSum();
        count += rating.getCount();
    }

    @Override
    public int compareTo(Rating otherRating) {
        int result = getAvg().compareTo(otherRating.getAvg());
        return result != 0 ? - result : - Long.compare(getCount(), otherRating.getCount()) ;
    }

}
