package BigDataLab2;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



public class AverageSort {
    // Mapper
    public static class AverageCountMapper extends Mapper<Object, Text, Word, FloatWritable> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] valList = value.toString().split("\\s+");
            float aver = Float.parseFloat(valList[1].split(",")[0]);
            context.write(new Word(valList[0], aver), new FloatWritable(aver));
        }
    }

    // Reducer
    public static class AverageCountReducer extends Reducer<Word, FloatWritable, Text, FloatWritable> {
        @Override
        public void reduce(Word key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            float sum = 0;
            for(FloatWritable value: values)
                sum += value.get();
            context.write(new Text(key.getKey()), new FloatWritable(key.getAverage()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        String[] remainArgs = new GenericOptionsParser(config, args).getRemainingArgs();
        Job job = new Job(config, "AverageCount");
        job.setJarByClass(AverageSort.class);
        job.setMapperClass(AverageCountMapper.class);
        job.setReducerClass(AverageCountReducer.class);
        job.setNumReduceTasks(10);
        job.setMapOutputKeyClass(Word.class);
        job.setMapOutputValueClass(FloatWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(remainArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(remainArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
