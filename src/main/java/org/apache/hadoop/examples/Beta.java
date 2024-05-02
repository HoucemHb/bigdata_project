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

public class Beta {

    public static class BetaMapper extends Mapper<Object, Text, Text, DoubleWritable> {

        private Text group = new Text();
        private DoubleWritable stockReturnWritable = new DoubleWritable();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Parse input line
            String[] parts = value.toString().split(";");
            if (parts.length >= 11) {
                group.set(parts[1].trim()); // Utilisation de la colonne 2 comme groupe
                String stockReturnStr = parts[5].trim();

                // Check if the stock return value is a valid double
                try {
                    double stockReturn = Double.parseDouble(stockReturnStr);
                    context.write(group, new DoubleWritable(stockReturn));
                } catch (NumberFormatException e) {
                    // Skip this record or handle error as needed
                    System.err.println("Skipping invalid stock return value: " + stockReturnStr);
                }
            }
        }
    }

    public static class BetaReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        private DoubleWritable outputValue = new DoubleWritable();

        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            List<Double> stockReturns = new ArrayList<>();

            // Collect stock returns for each group
            for (DoubleWritable value : values) {
                stockReturns.add(value.get());
            }

            // Calculate beta for each group
            double beta = calculateBeta(stockReturns);
            outputValue.set(beta);
            context.write(key, outputValue);
        }

        private double calculateBeta(List<Double> stockReturns) {
            double meanStockReturn = calculateMean(stockReturns);
            double meanMarketReturn = calculateMean(createMarketReturns(stockReturns.size()));

            double covariance = calculateCovariance(stockReturns, meanStockReturn, meanMarketReturn);
            double varianceMarket = calculateVariance(createMarketReturns(stockReturns.size()));

            return covariance / varianceMarket;
        }

        private double calculateMean(List<Double> returns) {
            double sum = 0;
            for (Double ret : returns) {
                sum += ret;
            }
            return sum / returns.size();
        }

        private double calculateCovariance(List<Double> stockReturns, double meanStockReturn, double meanMarketReturn) {
            double sum = 0;
            for (int i = 0; i < stockReturns.size(); i++) {
                sum += (stockReturns.get(i) - meanStockReturn) * (i - meanMarketReturn);
            }
            return sum / (stockReturns.size() - 1);
        }

        private double calculateVariance(List<Double> returns) {
            double mean = calculateMean(returns);
            double sum = 0;
            for (Double ret : returns) {
                sum += Math.pow(ret - mean, 2);
            }
            return sum / (returns.size() - 1);
        }

        private List<Double> createMarketReturns(int size) {
            List<Double> marketReturns = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                marketReturns.add((double) i);
            }
            return marketReturns;
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 5) {
            System.err.println("Usage: Beta <input1> <input2> <input3> <input4> <output>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Beta Calculation");
        job.setJarByClass(Beta.class);
        job.setMapperClass(BetaMapper.class);
        job.setReducerClass(BetaReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        // Ajouter les chemins d'entrée
        for (int i = 0; i < 4; i++) {
            FileInputFormat.addInputPath(job, new Path(args[i]));
        }

        // Spécifier le répertoire de sortie
        FileOutputFormat.setOutputPath(job, new Path(args[4]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}