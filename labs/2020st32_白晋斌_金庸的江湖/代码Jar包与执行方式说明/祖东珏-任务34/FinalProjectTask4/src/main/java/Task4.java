import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Task4 {
    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        String[] remainArgs = new GenericOptionsParser(config, args).getRemainingArgs();
        String task3OutputPath = remainArgs[0];
        String task4OutputPath = remainArgs[1];
        long totalNode = 0;

        // PageRank Init
        Job job4_init = Job.getInstance(config, "PageRankInit");
        job4_init.setJarByClass(PageRank.class);
        job4_init.setInputFormatClass(KeyValueTextInputFormat.class);
        job4_init.setMapperClass(PageRank.PageRankInitMapper.class);
        job4_init.setReducerClass(PageRank.PageRankInitReducer.class);
        job4_init.setMapOutputKeyClass(Text.class);
        job4_init.setMapOutputValueClass(Text.class);
        job4_init.setOutputKeyClass(Text.class);
        job4_init.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job4_init, new Path(task3OutputPath));
        FileOutputFormat.setOutputPath(job4_init, new Path(task4OutputPath + "/iter_0"));

        job4_init.waitForCompletion(true);
        totalNode = job4_init.getCounters().findCounter("MyCounter", "totalNode").getValue();

        // PageRank Iter
        int maxIterTime = 100;
        Double maxRankDelta = 0.01D;
        int iter_time;
        for(iter_time = 1; iter_time <= maxIterTime; iter_time++) {
            config = new Configuration();
            // Damping coefficient
            config.setFloat("damping", 0.85F);
            config.setFloat("delta", maxRankDelta.floatValue());
            config.setLong("totalNode", totalNode);
            Job job4_iter = Job.getInstance(config, "PageRankIter" + iter_time);
            job4_iter.setJarByClass(PageRank.class);
            job4_iter.setInputFormatClass(KeyValueTextInputFormat.class);
            job4_iter.setMapperClass(PageRank.PageRankIterMapper.class);
            job4_iter.setReducerClass(PageRank.PageRankIterReducer.class);
            job4_iter.setMapOutputKeyClass(Text.class);
            job4_iter.setMapOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job4_iter, new Path(task4OutputPath + "/iter_" + (iter_time - 1)));
            FileOutputFormat.setOutputPath(job4_iter, new Path(task4OutputPath + "/iter_" + iter_time));

            job4_iter.waitForCompletion(true);

            long value = job4_iter.getCounters().findCounter("MyCounter", "PageRank").getValue();
            if(value == 0)
                break;
        }

        // PageRank View
        config = new Configuration();
        Job job4_viewer = Job.getInstance(config, "PageRankViewer");
        job4_viewer.setJarByClass(PageRank.class);
        job4_viewer.setInputFormatClass(KeyValueTextInputFormat.class);
        job4_viewer.setMapperClass(PageRank.PageRankViewerMapper.class);
        job4_viewer.setReducerClass(PageRank.PageRankViewerReducer.class);
        job4_viewer.setMapOutputKeyClass(DoubleWritable.class);
        job4_viewer.setMapOutputValueClass(Text.class);
        job4_viewer.setOutputKeyClass(Text.class);
        job4_viewer.setOutputValueClass(DoubleWritable.class);

        if(iter_time > maxIterTime)
            FileInputFormat.addInputPath(job4_viewer, new Path(task4OutputPath + "/iter_" + maxIterTime));
        else
            FileInputFormat.addInputPath(job4_viewer, new Path(task4OutputPath + "/iter_" + iter_time));
        FileOutputFormat.setOutputPath(job4_viewer, new Path(task4OutputPath + "/finalResult"));

        job4_viewer.waitForCompletion(true);
        System.exit(0);
    }
}
