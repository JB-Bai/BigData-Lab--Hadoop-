package BigDataLab2;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class TFIDF {
    public static class TFIDFMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String fileNumber = context.getConfiguration().get("fileNumber");
            String[] line = value.toString().split("\\s+");
            String word = line[0];
            // Get bookname and count
            String[] docList = line[1].split(",")[1].split(";");

            String preWriter = "";

            int freqCount = 0;
            for(String doc: docList) {
                String writer = doc.split(":")[0].split("[0-9]")[0];
                if(!preWriter.equals("") && !writer.equals(preWriter)) {
                    context.write(new Text(writer), new Text(word + "," + freqCount + "#" + Math.log(Float.parseFloat(fileNumber) / (docList.length + 1))));
                    freqCount = 0;
                }
                else {
                    freqCount += Integer.parseInt(doc.split(":")[1]);
                }
                preWriter = writer;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        String[] remainArgs = new GenericOptionsParser(config, args).getRemainingArgs();

        FileSystem fs = FileSystem.get(config);
        FileStatus[] inputFiles = fs.listStatus(new Path(remainArgs[2]));
        config.set("fileNumber", String.valueOf(inputFiles.length));

        Job job = new Job(config, "TFIDF");
        job.setJarByClass(TFIDF.class);
        job.setMapperClass(TFIDFMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(remainArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(remainArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
