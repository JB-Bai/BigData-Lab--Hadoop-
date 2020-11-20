import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
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

public class MyJoin {
    public static class JoinMapper extends Mapper<Object, Text,  OrderBean,Text> {

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit fileSplit=(FileSplit)context.getInputSplit();
            String fileName=fileSplit.getPath().getName().split(".txt")[0];
            String dataLine=value.toString();
            if(dataLine==null||dataLine.compareTo("")==0)
                return;
            OrderBean orderBean=new OrderBean();
            String pid="";
            if(fileName.compareTo("product")==0){
                String []columns=dataLine.split(" ");
                pid=columns[0];
                String pname=columns[1];
                String price=columns[2];
                orderBean.set("","",pid,pname,price,"");
                //context.write(new Text(pid), new Text("A#"+pname+"#"+price));
            }
            else if(fileName.compareTo("order")==0) {
                String []columns=dataLine.split(" ");
                String oid=columns[0];
                String odate=columns[1];
                pid=columns[2];
                String oamount=columns[3];
                orderBean.set(oid,odate,pid,"","",oamount);
            }
            context.write(orderBean,new Text(""));
        }
    }
    public static class JoinReducer extends Reducer<OrderBean,Text,Text,Text> {
        OrderBean productInfo=new OrderBean();
        @Override
        protected void reduce(OrderBean key, Iterable<Text> values,Context context) throws IOException,InterruptedException {
            if(key.type().compareTo("product")==0) {
                productInfo.setPrice(key.getPrice());
                productInfo.setPname(key.getPname());
            }
            else {
                key.setPname(productInfo.getPname());
                key.setPrice(productInfo.getPrice());
                for(Text value:values)
                    context.write(new Text(key.toString()),new Text(""));
            }

        }
    }
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "My Join");
        job.setJarByClass(MyJoin.class);
        //job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(JoinMapper.class);
        job.setReducerClass(JoinReducer.class);

        job.setGroupingComparatorClass(JoinComparator.class);
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
