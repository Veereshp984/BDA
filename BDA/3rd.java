import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WeatherAnalysis {

    public static class WeatherMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            if (line.startsWith("date")) return; // skip header

            String[] fields = line.split(",");
            if (fields.length != 4) return;

            String date = fields[0];
            String temp = fields[1];
            String humidity = fields[2];
            String condition = fields[3];

            context.write(new Text(date), new Text(temp + "," + humidity + "," + condition));
        }
    }

    public static class WeatherReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text date, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            int count = 0;
            double tempSum = 0, humiditySum = 0;
            List<String> conditions = new ArrayList<>();

            for (Text val : values) {
                String[] parts = val.toString().split(",");
                double temp = Double.parseDouble(parts[0]);
                double humidity = Double.parseDouble(parts[1]);
                String condition = parts[2];

                tempSum += temp;
                humiditySum += humidity;
                conditions.add(condition);
                count++;
            }

            double avgTemp = tempSum / count;
            double avgHumidity = humiditySum / count;

            // Get the most common condition
            String finalCondition = mostFrequent(conditions);
            String message = messageForCondition(finalCondition);

            String output = String.format("AvgTemp: %.1f°C, AvgHumidity: %.1f%%, Condition: %s | %s",
                    avgTemp, avgHumidity, finalCondition, message);

            context.write(date, new Text(output));
        }

        private String mostFrequent(List<String> list) {
            String maxItem = null;
            int maxCount = 0;
            for (String item : list) {
                int count = 0;
                for (String s : list) {
                    if (s.equals(item)) count++;
                }
                if (count > maxCount) {
                    maxCount = count;
                    maxItem = item;
                }
            }
            return maxItem;
        }

        private String messageForCondition(String condition) {
            condition = condition.toLowerCase();
            if (condition.contains("sunny")) return "It's a bright and sunny day!";
            if (condition.contains("rain")) return "Carry an umbrella, it's rainy.";
            if (condition.contains("cloudy")) return "A bit cloudy today. Maybe wear a light jacket.";
            return "Stay prepared for changing weather!";
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Weather Analysis");

        job.setJarByClass(WeatherAnalysis.class);
        job.setMapperClass(WeatherMapper.class);
        job.setReducerClass(WeatherReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));  // input path
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // output path

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

