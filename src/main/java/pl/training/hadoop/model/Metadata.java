package pl.training.hadoop.model;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.io.Serializable;

@RequiredArgsConstructor
@NoArgsConstructor
@Data
public class Metadata implements Serializable {

    @NonNull
    private Long id;
    @NonNull
    private String title;

}
