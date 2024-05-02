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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class BetaData {

    public static class BetaDataMapper extends Mapper<Object, Text, Text, DoubleWritable> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (!value.toString().startsWith("SEANCE")) { // Skip headers
                String[] parts = value.toString().split(";");
                if (parts.length > 5) { // Check sufficient parts exist
                    String stockCode = parts[1].trim(); // Group identifier
                    try {
                        double closingPrice = Double.parseDouble(parts[5].trim()); // Closing price
                        context.write(new Text(stockCode), new DoubleWritable(closingPrice));
                    } catch (NumberFormatException e) {
                        System.err
                                .println("Invalid closing price: " + parts[5].trim() + " in stock code: " + stockCode);
                    }
                }
            }
        }
    }

    public static class BetaDataReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        private List<Double> marketReturns = new ArrayList<>();
        private String marketIndex;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            marketIndex = conf.get("market_index");
            System.out.println("Reducer setup: Market Index is " + marketIndex);
        }

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            List<Double> stockReturns = new ArrayList<>();
            System.out.println("Reducing key: " + key.toString());

            if (key.toString().equals(marketIndex)) {
                for (DoubleWritable value : values) {
                    marketReturns.add(value.get());
                    System.out.println("Market return added: " + value.get());
                }
            } else {
                for (DoubleWritable value : values) {
                    stockReturns.add(value.get());
                }
                System.out.println("Stock returns count for " + key.toString() + ": " + stockReturns.size());
            }

            if (!marketReturns.isEmpty() && !stockReturns.isEmpty()) {
                double beta = calculateBeta(stockReturns, marketReturns);
                context.write(key, new DoubleWritable(beta));
                System.out.println("Beta calculated for " + key.toString() + ": " + beta);
            } else {
                context.write(key, new DoubleWritable(0.0));
                System.out.println("No data available to calculate beta for " + key.toString());
            }
        }

        private double calculateBeta(List<Double> stockReturns, List<Double> marketReturns) {
            double stockMean = calculateMean(stockReturns);
            double marketMean = calculateMean(marketReturns);
            double covariance = calculateCovariance(stockReturns, marketReturns, stockMean, marketMean);
            double marketVariance = calculateVariance(marketReturns, marketMean);
            return covariance / marketVariance;
        }

        private double calculateMean(List<Double> values) {
            double sum = 0;
            for (double value : values) {
                sum += value;
            }
            return sum / values.size();
        }

        private double calculateCovariance(List<Double> x, List<Double> y, double xMean, double yMean) {
            double sum = 0;
            if (x.size() != y.size()) {
                return 0; // safeguard against unaligned data
            }
            for (int i = 0; i < x.size(); i++) {
                sum += (x.get(i) - xMean) * (y.get(i) - yMean);
            }
            return sum / (x.size() - 1); // sample covariance
        }

        private double calculateVariance(List<Double> values, double mean) {
            double sum = 0;
            for (double value : values) {
                double diff = value - mean;
                sum += diff * diff;
            }
            return sum / (values.size() - 1); // sample variance
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: BetaDataMain <input paths...> <output path> <market_index>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        String marketIndex = args[args.length - 1];
        conf.set("market_index", marketIndex);

        Job job = Job.getInstance(conf, "Beta Data Analysis");
        job.setJarByClass(BetaData.class);
        job.setMapperClass(BetaDataMapper.class);
        job.setReducerClass(BetaDataReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        for (int i = 0; i < args.length - 2; i++) {
            FileInputFormat.addInputPath(job, new Path(args[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(args[args.length - 2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
