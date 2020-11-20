package BigDataLab2;


import java.io.IOException;
import java.util.Enumeration;
import java.util.StringTokenizer;
import java.util.Hashtable;

import com.sun.org.apache.xml.internal.security.keys.KeyInfo;
import javafx.animation.KeyValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;
import org.mockito.internal.matchers.Null;

public class BigDataLab2 {
    public static class Map extends Mapper<Object, Text, Text, IntWritable> {
        private Text keyInfo = new Text(); // 存储词和小说组合
        private IntWritable valueInfo = new IntWritable(); // 存储词频


        @Override
        protected void map(Object key, Text value, Context content) throws IOException, InterruptedException {
            // 获得<key,value>对所属的FileSplit对象
            FileSplit fileSplit = (FileSplit) content.getInputSplit();
            String fileName = fileSplit.getPath().getName().split("(.txt)|(.TXT)")[0];
            Hashtable hashtable = new Hashtable<String, Integer>();
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String word = itr.nextToken();
                if (hashtable.containsKey(word)) {
                    hashtable.put(word, new Integer(((Integer) hashtable.get(word)).intValue() + 1));
                } else {
                    hashtable.put(word, new Integer(1));
                }
            }
            Enumeration<String> words = hashtable.keys();
            while (words.hasMoreElements()) {
                String word = words.nextElement();
                keyInfo.set(word + "," + fileName);
                valueInfo.set(((Integer) hashtable.get(word)).intValue());
                content.write(keyInfo, valueInfo);
            }
        }
    }

    public static class Combine extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable valueInfo = new IntWritable();
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // 统计词频
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            valueInfo.set(sum);
            context.write(key, valueInfo);
        }
    }

    public static class Partition extends HashPartitioner<Text, Object> {
        private final static Text word = new Text();
        @Override
        public int getPartition(Text key, Object value, int numReduceTasks) {
            word.set(key.toString().split(",")[0]);
            return super.getPartition(word, value, numReduceTasks);
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, Text> {
        private Text keyInfo = new Text(); // 存储词和小说组合
        private Text valueInfo = new Text(); // 存储词频
        private int sum_of_frequency = 0;
        private int num_of_file_contain = 0;
        private  String lastWord= null;
        StringBuilder basicTask = new StringBuilder(); //format : filename1:frequency1;...
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // 生成文档列表
            String[] fileList =key.toString().split(",");
            if(lastWord!=null &&!fileList[0].equals(lastWord)){
                keyInfo.set(lastWord);
                float avgTimes0=(float)sum_of_frequency/(float)num_of_file_contain;
                //float avgTimes=(float)(Math.round(avgTimes0*100)/100);
                //String.format("%.2f",avgTimes0);
                //valueInfo.set(avgTimes+","+basicTask.toString());
                valueInfo.set(String.format("%.2f",avgTimes0)+","+basicTask.toString());
                context.write(keyInfo,valueInfo);
                sum_of_frequency=0;
                num_of_file_contain=0;
                basicTask.delete(0,basicTask.length());
            }
            int frequency_this_file =0;
            for (IntWritable value:values){
                frequency_this_file+=value.get();
            }
            num_of_file_contain+=1;
            sum_of_frequency+=frequency_this_file;
            if(num_of_file_contain!=1){
                basicTask.append(";");
            }
            basicTask.append(fileList[1]+":"+frequency_this_file);
            lastWord=fileList[0];
        }
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            keyInfo.set(lastWord);
            float avg0=(float)sum_of_frequency/(float)num_of_file_contain;
            //float avg=(float)(Math.round(avg0*100)/100);
            //valueInfo.set(avg+","+basicTask.toString());
            valueInfo.set(String.format("%.2f",avg0)+","+basicTask.toString());
            context.write(keyInfo,valueInfo);
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job=Job.getInstance(conf, "inverted index");
        job.setJarByClass(BigDataLab2.class);

        // 设置Map、Combine和Reduce处理类
        job.setMapperClass(Map.class);
        job.setCombinerClass(Combine.class);
        job.setPartitionerClass(Partition.class);
        job.setReducerClass(Reduce.class);

        // 设置Map输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 设置Reduce输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 设置输入和输出目录
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

