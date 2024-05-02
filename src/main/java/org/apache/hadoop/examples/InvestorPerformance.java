package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvestorPerformance {

    public static class StockDataMapper extends Mapper<Object, Text, Text, DoubleWritable> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] tokens = line.split(";");
            if (tokens.length >= 6) {
                String investor = tokens[0].trim(); // Investor
                try {
                    double closingPrice = Double.parseDouble(tokens[5].trim()); // Closing price
                    context.write(new Text(investor), new DoubleWritable(closingPrice));
                } catch (NumberFormatException e) {
                    // Handle invalid closing price
                    System.err.println("Skipping invalid closing price in line: " + line);
                }
            } else {
                // Handle invalid input line
                System.err.println("Invalid input line: " + line);
            }
        }
    }

    public static class StockDataReducer extends Reducer<Text, DoubleWritable, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            List<Double> closingPrices = new ArrayList<>();
            for (DoubleWritable value : values) {
                closingPrices.add(value.get());
            }

            // Calculate average
            double sum = 0;
            for (double price : closingPrices) {
                sum += price;
            }
            double average = sum / closingPrices.size();

            // Calculate volatility (standard deviation)
            double sumOfSquares = 0;
            for (double price : closingPrices) {
                sumOfSquares += Math.pow(price - average, 2);
            }
            double variance = sumOfSquares / closingPrices.size();
            double volatility = Math.sqrt(variance);

            context.write(key, new Text("Average: " + average + "\tVolatility: " + volatility));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 5) {
            System.err.println("Usage: InvestorPerformance <input1> <input2> <input3> <input4> <output>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Investor Performance and Volatility Calculation");
        job.setJarByClass(InvestorPerformance.class);
        job.setMapperClass(StockDataMapper.class);
        job.setReducerClass(StockDataReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        for (int i = 0; i < 4; i++) {
            FileInputFormat.addInputPath(job, new Path(args[i]));
        }

        FileOutputFormat.setOutputPath(job, new Path(args[4]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
