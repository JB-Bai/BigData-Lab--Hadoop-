package BigDataLab3;

import java.util.Vector;
import java.io.IOException;

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


public class BigDataLab3{

    private static final String DELIMITER = " ";

    public static class LeftJoinMapper extends Mapper<LongWritable,Text,Text,Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException {
            String fileName = ((FileSplit)context.getInputSplit()).getPath().getName().split("(.txt)|(.TXT)")[0];
            String line =value.toString();
            if (line == null || line.equals("")) return;
            String[] lines = line.split(DELIMITER);
            if(fileName.equals("order")){
                String id1=lines[0];
                String order_date2=lines[1];
                String pid3=lines[2];
                String number6=lines[3];
                context.write(new Text(pid3),new Text("order"+DELIMITER+id1+DELIMITER+order_date2+DELIMITER+pid3+DELIMITER+number6));
            }
            else if(fileName.equals("product")){
                String pid3=lines[0];
                String name4=lines[1];
                String price5=lines[2];
                context.write(new Text(pid3),new Text("product"+DELIMITER+pid3+DELIMITER+name4+DELIMITER+price5));
            }
        }
    }

    public static class LeftJoinReduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            Vector<String> order = new Vector<String>();
            Vector<String> product = new Vector<String>();

            for(Text each_val:values) {
                String each = each_val.toString();
                if(each.startsWith("order")) {
                    order.add(each);
                } else if(each.startsWith("product")) {
                    product.add(each);
                }
            }
            for(String ord:order){
                String[] ords=ord.split(DELIMITER);
                String id1=ords[1];
                String order_date2=ords[2];
                String pid3=key.toString();
                String number6=ords[4];
                if (product.size() == 0) {
                    context.write(new Text(id1+DELIMITER+order_date2 + DELIMITER + pid3+DELIMITER+"NULL"+DELIMITER+"NULL"+DELIMITER+number6), new Text());
                } else {
                    for(String prod:product){
                        String[] prods=prod.split(DELIMITER);
                        String name4=prods[2];
                        String price5=prods[3];
                        context.write(new Text(id1+DELIMITER+order_date2 + DELIMITER + pid3+DELIMITER+name4+DELIMITER+price5+DELIMITER+number6), new Text());
                    }

                }
            }
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Lab3-join-st32");
        job.setJarByClass(BigDataLab3.class);

        job.setMapperClass(LeftJoinMapper.class);
        job.setReducerClass(LeftJoinReduce.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 设置输入和输出目录
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}