import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Vector;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.util.HashMap;
import java.util.*;


//Task4 PageRank

public class task4 {
    public static class PreMapper extends Mapper<Object, Text,  Text,Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {//input: 人名\t[人名，权重|人名，权重]
            Double N=1.0/1253;
            context.write(new Text(value.toString().split("\t")[0]+"\t"+N.toString()),new Text(value.toString().split("\t")[1]));
        }
    }
    public static class PreReducer extends Reducer<Text,Text,Text,Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values,Context context) throws IOException,InterruptedException {
            for(Text value:values){
                context.write(key,value);
            }
        }
    }


    public static class Mapper5 extends Mapper<Object, Text,  Text,Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String name = value.toString().split("\t")[0];
            String pagerank = value.toString().split("\t")[1];
            String friends = value.toString().split("\t")[2]; //[a,0.1|b,0.2]
            String[] friend=friends.substring(1,friends.length()-1).split("\\|");
            HashMap<String,Double> friendMap=new HashMap<String,Double>();
            for(String f:friend){
                Double res=Double.valueOf(pagerank)*Double.valueOf(f.split(",")[1]);
                context.write(new Text(f.split(",")[0]),new Text("&"+res.toString()));
            }

            context.write(new Text(name),new Text(pagerank+"\t"+friends));

        }
    }

    public static class Reducer5 extends Reducer<Text,Text,Text,Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values,Context context) throws IOException,InterruptedException {
            HashMap<String,String> friendMap=new HashMap<String,String>();
            String friends="";
            Double weight=0.0;
            for(Text v:values){
                if(v.toString().contains("&")){
                    weight+=Double.valueOf(v.toString().substring(1));
                }
                else{
                    friends=v.toString().split("\t")[1];
                }
            }
            weight=weight*0.88+(1-0.88)*(1.0/1253);
            context.write(key,new Text(weight.toString()+"\t"+friends));
        }
    }

    public static class MapperFinal extends Mapper<Object, Text,  Text,Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String name = value.toString().split("\t")[0];
            String label = value.toString().split("\t")[1];
            context.write(new Text(name), new Text(label));
        }
    }




    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();

        String job1InputPath=args[0];
        String job1OutputPath=args[1];
        //String countSt=args[2];

        Job job = Job.getInstance(configuration, "Task4-pre");
        job.setJarByClass(task4.class);
        job.setMapperClass(PreMapper.class);
        job.setReducerClass(PreReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(job1InputPath));
        FileOutputFormat.setOutputPath(job, new Path(job1OutputPath+"/log0"));

        job.waitForCompletion(true);

        Integer count=15;//Integer.getInteger(countSt.substring(1));
        for(Integer i=0;i<count;i++){
            Job job2 = Job.getInstance(configuration, "Task4-"+i.toString());
            job2.setJarByClass(task4.class);
            job2.setMapperClass(Mapper5.class);
            job2.setReducerClass(Reducer5.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job2,new Path(job1OutputPath+"/log"+i.toString()));
            FileOutputFormat.setOutputPath(job2,new Path(job1OutputPath+"/log"+Integer.toString(i+1).toString()));

            job2.waitForCompletion(true);
        }

        Job job3 = Job.getInstance(configuration, "Task4-final");
        job3.setJarByClass(task4.class);
        job3.setMapperClass(MapperFinal.class);
        job3.setReducerClass(PreReducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job3, new Path(job1OutputPath+"/log"+count.toString()));
        FileOutputFormat.setOutputPath(job3, new Path(job1OutputPath+"/final"));


        System.exit(job3.waitForCompletion(true) ? 0 : 1);//注意分开写，阻塞后面的job
    }
}


