import java.io.*;
import java.net.URI;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    private List<double[]> centroids = new ArrayList<>();

    // Load centroids from DistributedCache
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] cacheFiles = context.getCacheFiles();
        if (cacheFiles != null && cacheFiles.length > 0) {
            for (URI uri : cacheFiles) {
                BufferedReader br = new BufferedReader(new FileReader(new File(uri.getPath())));
                String line;
                while ((line = br.readLine()) != null) {
                    centroids.add(parsePoint(line));
                }
                br.close();
            }
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        double[] point = parsePoint(value.toString());
        int nearest = findNearest(point, centroids);
        context.write(new IntWritable(nearest), value);
    }

    private double[] parsePoint(String line) {
        String[] parts = line.trim().split(",");
        double[] point = new double[parts.length];
        for (int i = 0; i < parts.length; i++) {
            point[i] = Double.parseDouble(parts[i]);
        }
        return point;
    }

    private int findNearest(double[] point, List<double[]> centroids) {
        int nearest = 0;
        double minDist = Double.MAX_VALUE;
        for (int i = 0; i < centroids.size(); i++) {
            double dist = 0;
            double[] c = centroids.get(i);
            for (int j = 0; j < point.length; j++) {
                dist += Math.pow(point[j] - c[j], 2);
            }
            if (dist < minDist) {
                minDist = dist;
                nearest = i;
            }
        }
        return nearest;
    }
}