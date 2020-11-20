package BigDataLab2;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;

public class InvertedIndex {
    // Mapper
    public static class InvertedIndexMapper extends Mapper<Object, Text, Text, IntWritable> {
        public static final IntWritable one = new IntWritable(1);
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            int pos = fileName.indexOf('.');
            if(pos > 0)
                fileName = fileName.substring(0, pos);
            Text word = new Text();
            StringTokenizer wordList = new StringTokenizer(value.toString());
            while(wordList.hasMoreTokens()) {
                // Combine word and filename
                word.set(wordList.nextToken() + "#" + fileName);
                context.write(word, one);
            }
        }
    }

    // Combiner
    public static class InvertedIndexCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
        private  IntWritable result = new IntWritable();
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable value: values)
                sum += value.get();
            result.set(sum);
            context.write(key, result);
        }
    }

    // Partitioner
    public static class InvertedIndexPartitoner extends HashPartitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numReduceTasks) {
            String word = key.toString().split("#")[0];
            return super.getPartition(new Text(word), value, numReduceTasks);
        }
    }

    // Reducer
    public static class InvertedIndexReducer extends Reducer<Text, IntWritable, Text, Text> {
        // Variables
        private String curWord = "";
        private String lastWord = "";
        private int countWord;
        private int countDoc;
        private StringBuilder output = new StringBuilder();
        private float average;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            countWord = 0;
            countDoc = 0;
            average = 0;
        }

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            curWord = key.toString().split("#")[0];
            if(!curWord.equals(lastWord)) {
                if(!lastWord.equals("")) {
                    output.setLength(output.length() - 1);
                    average = (float)countWord / countDoc;
                    context.write(new Text(lastWord), new Text(String.format("%.2f,%s", average, output)));
                    countDoc = 0;
                    countWord = 0;
                    output = new StringBuilder();
                }
                lastWord = curWord;
            }
            int sum = 0;
            for(IntWritable value: values)
                sum += value.get();
            output.append(key.toString().split("#")[1] + ":" + sum + ";");
            countWord += sum;
            countDoc += 1;
        }

        @Override
        public void cleanup(Context context) throws  IOException, InterruptedException {
            output.setLength(output.length() - 1);
            average = (float)countWord / countDoc;
            context.write(new Text(lastWord), new Text(String.format("%.2f,%s", average, output)));
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        String[] remainArgs = new GenericOptionsParser(config, args).getRemainingArgs();
        Job job = new Job(config, "InvertedIndex");
        job.setJarByClass(InvertedIndex.class);
        job.setMapperClass(InvertedIndexMapper.class);
        job.setCombinerClass(InvertedIndexCombiner.class);
        job.setReducerClass(InvertedIndexReducer.class);
        job.setNumReduceTasks(10);
        job.setPartitionerClass(InvertedIndexPartitoner.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(remainArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(remainArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
