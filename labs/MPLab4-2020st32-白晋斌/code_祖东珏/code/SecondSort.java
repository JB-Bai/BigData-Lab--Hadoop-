package BigDataLab4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Iterator;

public class SecondSort {
    // Mapper
    public static class SecondSortMapper extends Mapper<Text, Text, MyNum, NullWritable> {
        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            MyNum cur = new MyNum(Integer.parseInt(key.toString()), Integer.parseInt(value.toString()));
            context.write(cur, NullWritable.get());
        }
    }

    // Partitoner
    public static class SecondSortPartitioner extends HashPartitioner<MyNum, NullWritable> {
        @Override
        public int getPartition(MyNum key, NullWritable value, int numReduceTasks) {
            return key.getFirst() % numReduceTasks;
        }
    }

    // Group Comparator
    public static class SecondSortGroupComparator extends WritableComparator {
        protected SecondSortGroupComparator() {
            super(MyNum.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            return ((MyNum) a).getFirst().compareTo(((MyNum) b).getFirst());
        }
    }

    // Reducer
    public static class SecondSortReducer extends Reducer<MyNum, NullWritable, Integer, Integer> {
        @Override
        public void reduce(MyNum key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            for (NullWritable value : values) {
                context.write(key.getFirst(), key.getSecond());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        String[] remainArgs = new GenericOptionsParser(config, args).getRemainingArgs();
        Job job = new Job(config, "MySecondSort");

        job.setJarByClass(SecondSort.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setMapperClass(SecondSortMapper.class);
        job.setPartitionerClass(SecondSortPartitioner.class);
        job.setGroupingComparatorClass(SecondSortGroupComparator.class);
        job.setReducerClass(SecondSortReducer.class);
        job.setMapOutputKeyClass(MyNum.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Integer.class);
        job.setOutputValueClass(Integer.class);

        FileInputFormat.addInputPath(job, new Path(remainArgs[1]));
        FileOutputFormat.setOutputPath(job, new Path(remainArgs[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
