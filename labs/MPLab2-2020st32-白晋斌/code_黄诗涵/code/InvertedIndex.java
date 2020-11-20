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
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;

public class InvertedIndex {
    public static class SortMapper extends Mapper<Object,Text, Text,Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException,InterruptedException {
            FileSplit fileSplit=(FileSplit)context.getInputSplit();
            String fileName=fileSplit.getPath().getName().split(".txt")[0];
            System.out.println(fileName);
            HashMap<String,Integer> hashMap=new HashMap();
            StringTokenizer izer=new StringTokenizer(value.toString());
            while(izer.hasMoreTokens()) {
                String word=izer.nextToken();
                if(hashMap.containsKey(word))
                    hashMap.put(word,hashMap.get(word)+1);
                else
                    hashMap.put(word,1);
            }
            Iterator<String> iter = hashMap.keySet().iterator();
            while(iter.hasNext())
            {
                String keyword=iter.next();
                IntWritable freq=new IntWritable(hashMap.get(keyword));
                context.write(new Text(keyword+"@"+fileName),new Text(freq.toString()));   //word@file, number
            }
        }
    }
    public static class NewPartitioner extends HashPartitioner<Text, Object> {
        @Override
        public int getPartition(Text key, Object value,int numReduceTasks) {
            Text term = new Text(key.toString().split("@")[0]);
            return  super.getPartition(term, value, numReduceTasks);
        }
    }
    public static class SortReducer extends Reducer<Text,Text,Text,Text> {
        private String prevTerm=null;
        private StringBuilder postingList=new StringBuilder();
        private int frequence=0;
        private int fileNum=0;
        @Override
        protected void reduce(Text key, Iterable<Text> values,Context context) throws IOException,InterruptedException{
            String []strings=key.toString().split("@");
            String curTerm=strings[0];
            String fileName=strings[1];
        //    String curTerm=key.toString().substring(0, key.find("@"));
          //  String fileName=key.toString().substring(key.find("@")+1, key.getLength());
            if((prevTerm!=null)&&(curTerm.compareTo(prevTerm)!=0))
            {
                float aver=(float)frequence/(float) fileNum;
                context.write(new Text(prevTerm),new Text(Float.toString(aver)+','+postingList.toString()));
                postingList=new StringBuilder();
                frequence=0;
                fileNum=0;
            }
            fileNum+=1;
            int count=0;

            for (Text value:values){
                count+=Integer.parseInt(value.toString());
            }
            postingList.append(fileName+":"+count+';');
            frequence+=count;
            prevTerm= curTerm;
        }
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            float aver=(float)frequence/(float) fileNum;
            if(prevTerm==null)
                prevTerm="";
            context.write(new Text(prevTerm),new Text(Float.toString(aver)+','+postingList.toString()));
        }
    }
    public static void main(String[] args) throws Exception {
      //  try{
            Configuration configuration = new Configuration();
            Job job = Job.getInstance(configuration, "Inverted Index");
            job.setJarByClass(InvertedIndex.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setMapperClass(SortMapper.class);

            job.setPartitionerClass(NewPartitioner.class);
            job.setReducerClass(SortReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
      //  }catch(Exception e){e.printStackTrace();}
    }


}
