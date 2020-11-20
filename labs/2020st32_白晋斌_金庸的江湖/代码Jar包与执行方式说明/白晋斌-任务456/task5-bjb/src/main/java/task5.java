//task5      LPA 方法1

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

enum MyCounter {
    CORRUPTED_DATA_COUNTER,
    NORMAL_DATA_COUNTER
}


public class task5 {
    public static class PreMapper extends Mapper<Object, Text,  Text,Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String friends=value.toString().split("\t")[1];
            String[] friend=friends.substring(1,friends.length()-1).split("\\|");
            StringBuilder newFriends=new StringBuilder();
            for(String f:friend){
                newFriends.append(f.split(",")[0]).append(",").append(f).append("|");
            }
            newFriends.deleteCharAt(newFriends.length()-1);
            context.write(new Text(value.toString().split("\t")[0]),new Text(value.toString().split("\t")[0]+"\t["+newFriends+"]"));//key姓名，value 标签\t[a,a,0.1|b,b,0.2]
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
            String friends = value.toString().split("\t")[2]; //[a,a,0.1|b,b,0.2]
            String[] friend=friends.substring(1,friends.length()-1).split("\\|");
            HashMap<String,Double> friendMap=new HashMap<String,Double>();
            for(String f:friend){
                String fLabel=f.split(",")[1];
                Double fWeight=Double.valueOf(f.split(",")[2]);
                if(friendMap.containsKey(fLabel)){
                    Double weight=fWeight+friendMap.get(fLabel);
                    friendMap.put(fLabel,weight);
                }
                else{
                    friendMap.put(fLabel,fWeight);
                }
            }
            double nextVal=0.0;
            String nextLabel="";
            for(String k:friendMap.keySet())
            {
                if(friendMap.get(k)>nextVal){
                    nextVal=friendMap.get(k);
                    nextLabel=k;
                }
            }
            if(!label.equals(nextLabel))
                context.getCounter(MyCounter.NORMAL_DATA_COUNTER).increment(1);
            context.write(new Text(name),new Text(nextLabel+"\t"+friends));
            for(String f:friend){
                context.write(new Text(f.split(",")[0]),new Text(name+"&"+nextLabel));
            }
        }
    }

    public static class Reducer5 extends Reducer<Text,Text,Text,Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values,Context context) throws IOException,InterruptedException {
            HashMap<String,String> friendMap=new HashMap<String,String>();
            String friends="";
            for(Text v:values){
                if(v.toString().contains("&")){
                    friendMap.put(v.toString().split("&")[0],v.toString().split("&")[1]);
                }
                else{
                    friends=v.toString();
                }
            }
            String label=friends.split("\t")[0];
            String friend=friends.split("\t")[1];
            String[] fr=friend.substring(1,friend.length()-1).split("\\|");
            StringBuilder finalFriends=new StringBuilder();
            for(String f:fr){
                if(friendMap.get(f.split(",")[1])!=null){
                    finalFriends.append(f.split(",")[0]).append(",").append(friendMap.get(f.split(",")[1])).append(",").append(f.split(",")[2]).append("|");
                }
                else{
                    finalFriends.append(f.split(",")[0]).append(",").append(f.split(",")[1]).append(",").append(f.split(",")[2]).append("|");
                }
            }
            finalFriends.deleteCharAt(finalFriends.length()-1);
            context.write(key,new Text(label+"\t"+"["+finalFriends+"]"));//key姓名，value 标签\t[a,0.1|b,0.2]
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
        job.setJarByClass(task5.class);
        job.setMapperClass(PreMapper.class);
        job.setReducerClass(PreReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(job1InputPath));
        FileOutputFormat.setOutputPath(job, new Path(job1OutputPath+"/log0"));

        job.waitForCompletion(true);

        Integer count=50;//Integer.getInteger(countSt);
        Integer last=0;
        for(Integer i=0;i<count;i++){
            Job job2 = Job.getInstance(configuration, "Task5-"+i.toString());
            job2.setJarByClass(task5.class);
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
        job3.setJarByClass(task5.class);
        job3.setMapperClass(MapperFinal.class);
        job3.setReducerClass(PreReducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job3, new Path(job1OutputPath+"/log"+last.toString()));
        FileOutputFormat.setOutputPath(job3, new Path(job1OutputPath+"/final"));


        System.exit(job3.waitForCompletion(true) ? 0 : 1);//注意分开写，阻塞后面的job
    }
}