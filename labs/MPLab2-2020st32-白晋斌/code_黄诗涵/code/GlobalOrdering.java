import javafx.animation.KeyValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
/*
public class GlobalOrdering {
    public static class GOMapper extends Mapper<Text,Text, FloatWritable,Text> {
        @Override
        protected void map(Text key, Text value, Context context) throws IOException,InterruptedException {
            String averfreq=value.toString().split(",")[0];
            Text NewValue=new Text(key.toString()+","+value.toString().split(",")[1]);
            context.write(new FloatWritable(Float.parseFloat(averfreq)),NewValue);

        }
    }
    public static class GOReducer extends Reducer<FloatWritable,Text,Text,Text> {
        @Override
        protected void reduce(FloatWritable key, Iterable<Text> values,Context context) throws IOException,InterruptedException{
            for(Text value:values)
            {
                String newKey=value.toString().split(",")[0];
                String newValue=key.toString()+","+value.toString().split(",")[1];
                context.write(new Text(newKey),new Text(newValue));
            }
        }

    }
    public static void main(String[] args) throws Exception {
        //  try{
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "Global Ordering");
        job.setJarByClass(GlobalOrdering.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        job.setMapperClass(GOMapper.class);
        job.setReducerClass(GOReducer.class);

        job.setMapOutputKeyClass(FloatWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        //  }catch(Exception e){e.printStackTrace();}
    }
}
*/
public class GlobalOrdering {
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
        job.setJarByClass(GlobalOrdering.class);
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
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
