package BigDataLab3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class MyJoin {
    // Mapper
    public static class MyJoinMapper extends Mapper<LongWritable, Text, Record, NullWritable>  {
        private String filename;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            FileSplit fs = (FileSplit)context.getInputSplit();
            String[] tmp = fs.getPath().toString().split("/");
            filename = tmp[tmp.length - 1];
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] attributes = value.toString().split(" ");
            Record record = new Record();
            if(filename.equals("product.txt")) {
                if(attributes.length < 3)
                    throw new IOException("Product Attributes Wrong!");
                record.type = RecordType.Product;
                record.pid = Long.parseLong(attributes[0]);
                record.pname = attributes[1];
                record.price = Float.parseFloat(attributes[2]);
            }
            else if(filename.equals("order.txt")) {
                if(attributes.length < 4)
                    throw new IOException("Order Attributes Wrong!");
                record.type = RecordType.Order;
                record.oid = Long.parseLong(attributes[0]);
                record.odata = attributes[1];
                record.pid = Long.parseLong(attributes[2]);
                record.oamount = Long.parseLong(attributes[3]);
            }
            else
                throw new IOException("Unknown Filename!" + filename.toString());
            context.write(record, NullWritable.get());
        }
    }

    // Partitioner
    public static class MyJoinPartitioner extends HashPartitioner<Record, NullWritable> {
        @Override
        public int getPartition(Record key, NullWritable value, int numReduceTasks) {
            return key.pid.intValue() % numReduceTasks;
        }
    }

    // Reducer
    public static class MyJoinReducer extends Reducer<Record, NullWritable, Record, NullWritable> {
        private String curPname;
        private Float curPrice;
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            curPname = " ";
            curPrice = 0F;
        }

        @Override
        public void reduce(Record key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            if(key.type == RecordType.Product) {
                curPname = key.pname;
                curPrice = key.price;
            }
            else if(key.type == RecordType.Order) {
                key.pname = curPname;
                key.price = curPrice;
                context.write(key, NullWritable.get());
            }
            else
                throw new IOException("Record type Unknown!");
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        String[] remainArgs = new GenericOptionsParser(config, args).getRemainingArgs();
        Job job = new Job(config, "MyJoin");
        job.setJarByClass(MyJoin.class);
        job.setMapperClass(MyJoinMapper.class);
        job.setPartitionerClass(MyJoinPartitioner.class);
        job.setReducerClass(MyJoinReducer.class);
        job.setMapOutputKeyClass(Record.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Record.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(remainArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(remainArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
