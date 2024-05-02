package org.apache.hadoop.examples;

import java.io.IOException;
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
import java.util.ArrayList;
import java.util.List;

public class StockMoyenne {

    public static class StockDataMapper extends Mapper<Object, Text, Text, DoubleWritable> {

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

    public static class StockDataReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            List<Double> prices = new ArrayList<>();
            for (DoubleWritable value : values) {
                prices.add(value.get());
            }

            // Calculate the sum of prices and count
            double sum = 0;
            int count = 0;
            for (double price : prices) {
                sum += price;
                count++;
            }

            // Calculate the mean
            double mean = sum / count;

            context.write(key, new DoubleWritable(mean));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: Stock Moyenne <input1> <input2> ... <output>");
            System.exit(1);
        }

        List<String> inputPaths = new ArrayList<>();
        for (int i = 0; i < args.length - 1; i++) {
            inputPaths.add(args[i]);
        }
        String outputPath = args[args.length - 1];

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Stock Data Analysis");
        job.setJarByClass(StockMoyenne.class);
        job.setMapperClass(StockDataMapper.class);
        job.setReducerClass(StockDataReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        for (String inputPath : inputPaths) {
            FileInputFormat.addInputPath(job, new Path(inputPath));
        }
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
