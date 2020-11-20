import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
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
import java.text.DecimalFormat;
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

public class TFIDF {

    public static class TFIDFMapper extends Mapper<Object,Text, Text,Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException,InterruptedException {

            FileSplit fileSplit=(FileSplit)context.getInputSplit();
            String fileName=fileSplit.getPath().getName().split(".txt")[0];
            System.out.println(fileName);
            HashMap<String,Integer> hashMap=new HashMap();
            StringTokenizer izer=new StringTokenizer(value.toString());
          //  int curNum=Integer.parseInt(context.getConfiguration().get("fileNum"))+1;
           // context.getConfiguration().setStrings("fileNum",Integer.toString(curNum));
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

    public static class TFIDFCombine extends Reducer<Text, Text, Text, Text> {
        private String prevTerm=null;
        private StringBuilder postingList=new StringBuilder();
        private int frequence=0;
        private int fileNum=0;
        @Override
        protected void reduce(Text key, Iterable<Text> values,Context context) throws IOException,InterruptedException{
            String []strings=key.toString().split("@");
            String curTerm=strings[0];
            String fileName=strings[1];
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

    public static class TFIDFReducer extends Reducer<Text,Text,Text,Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws
                IOException, InterruptedException {
            String word=key.toString();
            HashMap<String,Integer> hashMap=new HashMap<String, Integer>();
            int wordfileNum=0;
            int fileNum=Integer.parseInt(context.getConfiguration().get("fileNum"));
            for(Text value:values){
                String[] files = value.toString().split(",")[1].split(";");
                wordfileNum+=files.length;
                for (String file:files)
                {
                    String author=file.split("[0-9]")[0];
                    String freq=file.split(":")[1];
                    if(hashMap.containsKey(author))
                        hashMap.put(author,hashMap.get(author)+Integer.parseInt(freq));
                    else
                        hashMap.put(author,Integer.parseInt(freq));
                }
              //  context.write(new Text(buf[0]), new Text(buf[1]));
            }
            Iterator<String> iter = hashMap.keySet().iterator();

            while(iter.hasNext())
            {
                String keyword=iter.next();
                String TF=hashMap.get(keyword).toString();
                DecimalFormat df = new DecimalFormat("#.00");
                double  IDF=Math.log((float)fileNum/((float)wordfileNum+1))/Math.log(2);
                double tfidf=Integer.parseInt(TF)*IDF;
                context.write(new Text(keyword+","+ key+","+df.format(tfidf)),new Text(""));   //word@file, number
            }
        }
    }
    public static void main(String[] args) throws Exception {
        //  try{
        Configuration configuration = new Configuration();
        int num=0;
        FileSystem fs = FileSystem.get(configuration);
        FileStatus[] status = fs.listStatus(new Path(args[0]));
        for (FileStatus file : status) {
            if(file.isFile())
                num++;
        }



        configuration.setStrings("fileNum",Integer.toString(num));
        Job job = Job.getInstance(configuration, "Compute TF-IDF");
        job.setJarByClass(TFIDF.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(TFIDFMapper.class);
        job.setPartitionerClass(NewPartitioner.class);
        job.setCombinerClass(TFIDFCombine.class);
        job.setReducerClass(TFIDFReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        //  }catch(Exception e){e.printStackTrace();}
    }


}
