import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class SecondSort {
    public static class SortMapper extends Mapper<Object, Text,  PairBean,Text> {

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit fileSplit=(FileSplit)context.getInputSplit();
            StringTokenizer izer=new StringTokenizer(value.toString());
            while(izer.hasMoreTokens()) {
                Integer first=Integer.parseInt(izer.nextToken());
                Integer second=Integer.parseInt(izer.nextToken());
                PairBean bean=new PairBean(first,second);
                context.write(bean,new Text(""));
            }

        }
    }
    public static class SortReducer extends Reducer<PairBean,Text,Text,Text> {
        @Override
        protected void reduce(PairBean key, Iterable<Text> values,Context context) throws IOException,InterruptedException {

                    context.write(new Text(key.toString()),new Text(""));

        }
    }
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "Second Sort");
        job.setJarByClass(SecondSort.class);
        //job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);

        job.setGroupingComparatorClass(SecondSortComparator.class);
        job.setOutputKeyClass(PairBean.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
