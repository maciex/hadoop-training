package pl.training.hadoop.spark;

public class Hdfs {

    private String basePath;

    public Hdfs(String basePath) {
        this.basePath = basePath;
    }

    public String absolutePath(String file) {
        return String.format(basePath, file);
    }

}
