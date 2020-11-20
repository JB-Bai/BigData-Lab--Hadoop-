import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import java.io.*;
import java.util.*;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;

public class Task3 {
    //Job3 Normalization
    public static class NormMapper extends Mapper<Text, Text, Text, IntWritable> {
        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key, new IntWritable(Integer.parseInt(value.toString())));
        }
    }

    public static class NormPartitioner extends HashPartitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numReduceTasks) {
            String word = key.toString().split(",")[0];
            return super.getPartition(new Text(word), value, numReduceTasks);
        }
    }

    static class MyCharactor {
        public String name;
        public int occur;
        MyCharactor() {
            name = "";
            occur = 0;
        }
    }

    public static class NormReducer extends Reducer<Text, IntWritable, Text, Text> {
        private String curChara, lastChara;
        private long occurSum;
        private Vector<MyCharactor> charaCache;
        private StringBuilder output;

        // Write result into output file
        private void writeResult(Context context) throws IOException, InterruptedException {
            output.append("[");
            for(MyCharactor cur: charaCache) {
                output.append(cur.name).append(String.format(",%.10f", (double)cur.occur / (double) occurSum)).append('|');
            }
            output.deleteCharAt(output.length() - 1);
            output.append("]");
            context.write(new Text(lastChara), new Text(output.toString()));
        }

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            occurSum = 0L;
            lastChara = "Invalid";
            curChara = "";
            charaCache = new Vector<MyCharactor>();
            output = new StringBuilder();
        }

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            curChara = key.toString().split(",")[0];
            if(!curChara.equals(lastChara) && !lastChara.equals("Invalid")) {
                writeResult(context);
                charaCache.clear();
                occurSum = 0L;
                output = new StringBuilder();
            }
            lastChara = curChara;
            MyCharactor tmp = new MyCharactor();
            tmp.name = key.toString().split(",")[1];
            int sum = 0;
            for(IntWritable value: values)
                sum += value.get();
            tmp.occur = sum;
            occurSum += sum;
            charaCache.add(tmp);
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            writeResult(context);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        String[] remainArgs = new GenericOptionsParser(config, args).getRemainingArgs();
        //job3 Normalization
        String job3InputPath = remainArgs[0];
        String job3OutputPath = remainArgs[1];

        Job job3 = Job.getInstance(config, "Normalization");
        job3.setJarByClass(Task3.class);
        job3.setInputFormatClass(KeyValueTextInputFormat.class);
        job3.setMapperClass(Task3.NormMapper.class);
        job3.setPartitionerClass(Task3.NormPartitioner.class);
        job3.setReducerClass(Task3.NormReducer.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(IntWritable.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job3, new Path(job3InputPath));
        FileOutputFormat.setOutputPath(job3, new Path(job3OutputPath));
        job3.waitForCompletion(true);
    }

}
