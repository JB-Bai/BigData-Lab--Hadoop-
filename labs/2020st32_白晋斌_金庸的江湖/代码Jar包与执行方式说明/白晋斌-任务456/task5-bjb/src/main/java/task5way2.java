
//task5   方法2 仿照PageRank

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;


public class task5way2 {
    public static class PreMapper extends Mapper<Object, Text,  Text,Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            context.write(new Text(value.toString().split("\t")[0]),value);//key姓名，value 标签\t[a,0.1|b,0.2]
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
            String label = value.toString().split("\t")[1];
            String friends = value.toString().split("\t")[2];
            context.write(new Text(name), new Text("#" +label+"#"+ friends));
            String[] perFriend = friends.substring(1, friends.length() - 1).split("\\|");
            for (String friend : perFriend) {
                context.write(new Text(friend.split(",")[0]),
                        new Text(label + "|" + friend.split(",")[1]));
            }
        }
    }
    public static class Reducer5 extends Reducer<Text,Text,Text,Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values,Context context) throws IOException,InterruptedException {
            String name=key.toString();
            String friends="";
            HashMap<String, Double> labelAndWeight = new HashMap<String, Double>();
            double val=0.0;
            String label="";
            String preLabel="";
            for(Text value:values){
                String v=value.toString();
                if(v.startsWith("#")){
                    friends=v.substring(1,v.length()).split("#")[1];
                    preLabel=v.substring(1,v.length()).split("#")[0];
                }
                else{
                    if(labelAndWeight.containsKey(v.split("\\|")[0])) {
                        Double tmp = Double.valueOf(v.split("\\|")[1]) + labelAndWeight.get(v.split("\\|")[0]);
                        labelAndWeight.put(v.split("\\|")[0], tmp);
                    }
                    else{
                        labelAndWeight.put(v.split("\\|")[0],Double.valueOf(v.split("\\|")[1]));
                    }
                }
            }
            for(String k:labelAndWeight.keySet())
            {
                if(labelAndWeight.get(k)>val){
                    val=labelAndWeight.get(k);
                    label=k;
                }
            }
            if(!label.equals(preLabel))
                context.getCounter(MyCounter.NORMAL_DATA_COUNTER).increment(1);
            context.write(key,new Text(label+"\t"+friends));//key姓名，value 标签\t[a,0.1|b,0.2]
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

        Job job = Job.getInstance(configuration, "Task5-pre");
        job.setJarByClass(task5way2.class);
        job.setMapperClass(PreMapper.class);
        job.setReducerClass(PreReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(job1InputPath));
        FileOutputFormat.setOutputPath(job, new Path(job1OutputPath+"/log0"));

        job.waitForCompletion(true);

        Integer count=30;//Integer.getInteger(countSt);
        Integer last=0;
        for(Integer i=0;i<count;i++){
            Job job2 = Job.getInstance(configuration, "Task5-"+i.toString());
            job2.setJarByClass(task5way2.class);
            job2.setMapperClass(Mapper5.class);
            job2.setReducerClass(Reducer5.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job2,new Path(job1OutputPath+"/log"+i.toString()));
            FileOutputFormat.setOutputPath(job2,new Path(job1OutputPath+"/log"+Integer.toString(i+1).toString()));

            job2.waitForCompletion(true);
            last=i+1;
            System.out.println("\n"+"第"+i+"次：counter = "+job2.getCounters().findCounter(MyCounter.NORMAL_DATA_COUNTER).getValue());
            if(job2.getCounters().findCounter(MyCounter.NORMAL_DATA_COUNTER).getValue()<=0)
                break;

        }

        Job job3 = Job.getInstance(configuration, "Task5-final");
        job3.setJarByClass(task5way2.class);
        job3.setMapperClass(MapperFinal.class);
        job3.setReducerClass(PreReducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job3, new Path(job1OutputPath+"/log"+last.toString()));
        FileOutputFormat.setOutputPath(job3, new Path(job1OutputPath+"/final"));


        System.exit(job3.waitForCompletion(true) ? 0 : 1);//注意分开写，阻塞后面的job
    }
}


//反思1：注意分开写，阻塞后面的job
//反思2：split("|")要写成\\|
