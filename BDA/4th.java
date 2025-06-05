import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MovieTags {

    public static class TagMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            // Skip header
            if (line.startsWith("userId")) return;

            String[] fields = line.split(",", 4);
            if (fields.length >= 3) {
                String movieId = fields[1];
                String tag = fields[2].trim();
                if (!tag.isEmpty()) {
                    context.write(new Text(movieId), new Text(tag));
                }
            }
        }
    }

    public static class TagReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text movieId, Iterable<Text> tags, Context context) throws IOException, InterruptedException {
            List<String> tagList = new ArrayList<>();
            for (Text tag : tags) {
                tagList.add(tag.toString());
            }
            context.write(movieId, new Text(tagList.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Movie Tags");

        job.setJarByClass(MovieTags.class);
        job.setMapperClass(TagMapper.class);
        job.setReducerClass(TagReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}