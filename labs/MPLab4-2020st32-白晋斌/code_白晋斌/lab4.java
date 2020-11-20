package lab4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.mockito.internal.matchers.Or;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.StringTokenizer;

public class lab4 {

    public static class OrderBean implements WritableComparable<OrderBean> {
        private Integer first;
        private Integer second;
        public OrderBean(){

        }

        //public OrderBean(Integer left, Integer right) {
        //    this.first = new Integer(left);
        //    this.second = new Integer(right);
        //}
        public void set(Integer left, Integer right) {
            this.first = new Integer(left);
            this.second = new Integer(right);
        }
        public int getFirst() {
            return first;
        }

        public int getSecond() {
            return second;
        }

        @Override
        public int compareTo(OrderBean o) {
            if (!this.first.equals(o.first)) {
                return this.first.compareTo(o.first);  // < o.first ? -1 : 1;
            } else if (!this.second.equals(o.second)) {
                return -this.second.compareTo(o.second);  //< o.second ? -1 : 1;
            } else {
                return 0;
            }
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.first = in.readInt();
            this.second = in.readInt();
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(this.first);
            out.writeInt(this.second);
        }
    }


    public static class SortMapper extends Mapper<Object,Text,OrderBean,Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line =value.toString();
            if (line == null || line.equals("")) return;
            String[] v = line.split("\t");
            OrderBean o=new OrderBean();
            o.set(Integer.parseInt(v[0]),Integer.parseInt(v[1]));
            context.write(o, new Text());
        }
    }
    public static class SortPartitioner extends Partitioner<OrderBean, NullWritable> {
        @Override
        public int getPartition(OrderBean key, NullWritable value, int numPartitions) {

            return key.getFirst() % numPartitions;
        }
    }
    public static class SortReducer extends Reducer<OrderBean,Text,IntWritable,IntWritable> {
        @Override
        protected void reduce(OrderBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text value:values){
                context.write(new IntWritable(key.getFirst()),new IntWritable(key.getSecond()));
            }
        }

    }

    public static class SortComparator extends WritableComparator {
        public SortComparator() {
            super(OrderBean.class,true);
        }
        public int compare(OrderBean a,OrderBean b)
        {
            return a.compareTo(b);
        }
    }
/*
    public static class SortMapper extends Mapper<Text,Text,IntWritable,IntWritable> {
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String[] v= value.toString().split("\t");
            context.write(new IntWritable(Integer.parseInt(v[0])), new IntWritable(Integer.parseInt(v[1])));
        }
    }
    public static class SortReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            Iterator<IntWritable> itr = values.iterator();

            while (itr.hasNext()) {
                String tmp = itr.next().toString();
                String[] buf = tmp.split("#");
                context.write(new Text(buf[0]), new Text(buf[1]));
            }

        }
    }
*/




    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job=Job.getInstance(conf, "lab4-st32");
        job.setJarByClass(lab4.class);

        //job.setNumReduceTasks(5);
        // 设置Map、Combine和Reduce处理类
        job.setMapperClass(SortMapper.class);
        job.setPartitionerClass(SortPartitioner.class);
        job.setReducerClass(SortReducer.class);
        job.setGroupingComparatorClass(SortComparator.class);
        // 设置Map输出类型
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(Text.class);

        // 设置Reduce输出类型
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        // 设置输入和输出目录
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}
