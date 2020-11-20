    package BigDataLab2;

import java.util.Map;
import java.util.Map.Entry;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//ques1: 比较，lab2-1， 1个reduce
//ques2: 小数位数


public class BigDataLab2_2 {

    public static class myMap extends Mapper<Text,Text, Text,Text> {
        @Override
        protected void map(Text key,Text value,Context context) throws IOException,InterruptedException{
            String []termFreqs=value.toString().split(",")[1].split(";"); //书名：频率
            String fileNumber = context.getConfiguration().get("fileNumbers");
            double idf=Math.log((double)Integer.parseInt(fileNumber)/(termFreqs.length+1));
            Map<String, Integer> authorMap=new HashMap<String,Integer>();
            for(String term:termFreqs){
                String author=term.split(":")[0].split("[0-9]")[0];
                int freq=Integer.parseInt((term.split(":"))[1]);
                Integer ins= authorMap.get(author);
                if (ins == null)
                {
                    ins = 0;
                }
                authorMap.put(author,ins+freq);
            }
            for(Map.Entry<String,Integer>entry:authorMap.entrySet()){
                Text word=new Text();
                word.set(entry.getKey());
                if(!entry.getKey().equals("")){
                    context.write(word,new Text(key+"\t"+String.format("%.2f",entry.getValue()*idf))); //+"\t"+fileNumber+"\t"+termFreqs.length));
                }
            }
        }



    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //int fileNumbers=FileSystem.listStatus(Path f)
        //FileStatus[] files = FileSystem.listStatus(path);

        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(conf);
        FileStatus[] inputFiles = fs.listStatus(new Path(args[2]));
        //String fileNumbers=null;
        //conf.set(fileNumbers,String.valueOf(inputFiles.length));
        conf.set("fileNumbers",String.valueOf(inputFiles.length));

        Job job=Job.getInstance(conf, "optional-2");
        job.setJarByClass(BigDataLab2.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        // 设置Map、Combine和Reduce处理类
        job.setMapperClass(myMap.class);

        // 设置Map输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);


        // 设置输入和输出目录
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
