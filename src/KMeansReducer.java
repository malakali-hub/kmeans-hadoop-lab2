import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class KMeansReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        int dimension = -1;
        int count = 0;
        double[] sum = null;

        for (Text val : values) {
            double[] point = parsePoint(val.toString());
            if (sum == null) {
                dimension = point.length;
                sum = new double[dimension];
            }
            for (int i = 0; i < dimension; i++) {
                sum[i] += point[i];
            }
            count++;
        }

        for (int i = 0; i < dimension; i++) {
            sum[i] /= count;
        }

        context.write(key, new Text(toString(sum)));
    }

    private double[] parsePoint(String line) {
        String[] parts = line.trim().split(",");
        double[] point = new double[parts.length];
        for (int i = 0; i < parts.length; i++) {
            point[i] = Double.parseDouble(parts[i]);
        }
        return point;
    }

    private String toString(double[] point) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < point.length; i++) {
            if (i > 0) sb.append(",");
            sb.append(point[i]);
        }
        return sb.toString();
    }
}