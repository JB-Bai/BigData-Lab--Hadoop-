package BigDataLab2;


import java.io.IOException;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.Hashtable;

import com.sun.org.apache.xml.internal.security.keys.KeyInfo;
import javafx.animation.KeyValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapred.lib.InputSampler;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;
import org.mockito.internal.matchers.Null;

public class BigDataLab2_1 {
    public static class SortMapper extends Mapper<Text,Text,FloatWritable,Text> {
        @Override
        protected void map(Text key, Text value, Context context) throws
                IOException, InterruptedException {
            String num = value.toString().split(",")[0];
            FloatWritable newKey = new FloatWritable(Float.parseFloat((num)));
            String tmpValue = key.toString() + "#" + value.toString();
            context.write(newKey, new Text(tmpValue));
        }

    }


    public static class SortReducer extends Reducer<FloatWritable,Text,Text,Text> {
        @Override
        protected void reduce(FloatWritable key, Iterable<Text> values, Context context) throws
                IOException, InterruptedException {
            Iterator<Text> itr = values.iterator();
            while (itr.hasNext()) {
                String tmp = itr.next().toString();
                String[] buf = tmp.split("#");
                context.write(new Text(buf[0]), new Text(buf[1]));
            }

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job=Job.getInstance(conf, "optional-1");
        job.setJarByClass(BigDataLab2.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        // 设置Map、Combine和Reduce处理类
        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);

        // 设置Map输出类型
        job.setMapOutputKeyClass(FloatWritable.class);
        job.setMapOutputValueClass(Text.class);

        // 设置Reduce输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 设置输入和输出目录
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //设置全排序
        //创建随机采样器对象
        //freq:每个key被选中的概率,概率为1则是全选
        //numSample：抽取样本的总数
        //maxSplitSampled:最大采样切片数
        InputSampler.Sampler<FloatWritable,Text>sampler=
                new InputSampler.RandomSampler<FloatWritable, Text>(0.5,10000,10);

        //job.setNumReduceTasks(3);//reduce个数，它会根据我们重定义的分区方式进行将数据进行分区，在每个分区上做mapreduce操作，即最终得到的结果在整体上也是有序的

        //将sample数据写入分区文件
        Path partitionFile = new Path(args[2]);
        TotalOrderPartitioner.setPartitionFile(job.getConfiguration(),partitionFile);
        job.setPartitionerClass(TotalOrderPartitioner.class);//设置全排序分区类

        InputSampler.writePartitionFile(job,sampler);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
