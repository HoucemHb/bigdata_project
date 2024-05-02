package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class StockVolatility {

    public static class VolatilityMapper extends Mapper<Object, Text, Text, DoubleWritable> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Skip the first line containing the column headers
            if (!value.toString().startsWith("SEANCE")) {
                String[] parts = value.toString().split(";");
                if (parts.length >= 6) { // Assuming there are at least 6 parts in the input data
                    String sector = parts[3].trim(); // Extract the sector from the 4th column (index 3)
                    String closingPriceStr = parts[5].trim(); // Extract the closing price from the 6th column (index 5)
                    try {
                        double closingPrice = Double.parseDouble(closingPriceStr);
                        context.write(new Text(sector), new DoubleWritable(closingPrice));
                    } catch (NumberFormatException e) {
                        // Log or handle the invalid closing price
                        System.err.println("Invalid closing price: " + closingPriceStr + ". Skipping this record.");
                    }
                }
            }
        }

    }

    public static class VolatilityReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            List<Double> prices = new ArrayList<>();
            for (DoubleWritable value : values) {
                prices.add(value.get());
            }

            // Calculate standard deviation
            double volatility = calculateStandardDeviation(prices);
            System.out.println("Volatility: " + volatility);
            context.write(key, new DoubleWritable(volatility));
        }

        private double calculateStandardDeviation(List<Double> values) {
            double mean = calculateMean(values);
            double sumSquaredDiff = 0;
            for (double value : values) {
                double diff = value - mean;
                sumSquaredDiff += diff * diff;
            }
            double variance = sumSquaredDiff / (values.size() - 1);
            return Math.sqrt(variance);
        }

        private double calculateMean(List<Double> values) {
            double sum = 0;
            for (double value : values) {
                sum += value;
            }
            return sum / values.size();
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: StockData <input1> <input2> ... <output>");
            System.exit(1);
        }

        List<String> inputPaths = new ArrayList<>();
        for (int i = 0; i < args.length - 1; i++) {
            inputPaths.add(args[i]);
        }
        String outputPath = args[args.length - 1];

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Stock Data Analysis");
        job.setJarByClass(StockVolatility.class);
        job.setMapperClass(VolatilityMapper.class);
        job.setReducerClass(VolatilityReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        for (String inputPath : inputPaths) {
            FileInputFormat.addInputPath(job, new Path(inputPath));
        }

        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        FileOutputFormat.setCompressOutput(job, false);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
