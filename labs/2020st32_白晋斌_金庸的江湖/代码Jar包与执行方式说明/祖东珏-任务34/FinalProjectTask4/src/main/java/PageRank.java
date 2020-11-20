import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PageRank {
    // Init
    public static class PageRankInitMapper extends Mapper<Text, Text, Text, Text> {
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String adjListStr = value.toString();
            adjListStr = "1.0\t" + adjListStr.substring(1, adjListStr.length() - 1);
            context.write(key, new Text(adjListStr));
            context.getCounter("MyCounter", "totalNode").increment(1L);
        }
    }

    public static class PageRankInitReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text value: values) {
                context.write(key, value);
            }
        }
    }

    // Iter
    public static class PageRankIterMapper extends Mapper<Text, Text, Text, Text> {
        // adjList structure "A \t 1.0 \t B,0.1|C,0.8|D,0.1"
        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            double pageRank = 0;

            context.write(key, value);

            String adjListStr = value.toString();
            pageRank = Double.parseDouble(adjListStr.split("\t")[0]);

            String[] adjList = adjListStr.split("\t")[1].split("\\|");
            // Emit adjacent url and weight
            for(String url: adjList) {
                String name = url.split(",")[0];
                String weight = url.split(",")[1];
                context.write(new Text(name), new Text(String.valueOf(pageRank * Double.parseDouble(weight))));
            }
        }
    }

    public static class PageRankIterReducer extends Reducer<Text, Text, Text, Text> {
        private double delta;
        private double damping;
        private double maxDelta;
        private long totalNode;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            damping = context.getConfiguration().getFloat("damping", 0.85F);
            delta = context.getConfiguration().getFloat("delta", 0.01F);
            maxDelta = 0.0D;
            totalNode = context.getConfiguration().getLong("totalNode", 0L);
            context.getCounter("MyCounter", "PageRank").setValue(0L);
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double rankSum = 0.0D, oldPageRank = 0.0D;
            String adjList = "";
            for(Text node: values) {
                String[] judge = node.toString().split("\t");
                if(judge.length == 1) {
                    rankSum += Double.parseDouble(judge[0]);
                }
                else {
                    // Remove the old pagerank
                    adjList = judge[1];
                    oldPageRank = Double.parseDouble(judge[0]);
                }
            }

            double newPageRank = (1 - damping) / totalNode + damping * rankSum;
            double diff = Math.abs(newPageRank - oldPageRank);

            if(diff > maxDelta)
                maxDelta = diff;

            adjList = Double.toString(newPageRank) + '\t' + adjList;
            context.write(key, new Text(adjList));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if(maxDelta > delta) {
                Counter myCounter = context.getCounter("MyCounter", "PageRank");
                myCounter.increment(1L);
            }
        }
    }

    // Sort the pagerank result
    public static class PageRankViewerMapper extends Mapper<Text, Text, DoubleWritable, Text> {
        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            double pageRank = 0;
            String adjListStr = value.toString();
            String[] adjList = adjListStr.substring(1, value.toString().length() - 1).split("\\|");
            pageRank = Double.parseDouble(adjList[0].split("\t")[0]);
            context.write(new DoubleWritable(-pageRank), key);
        }
    }

    public static class PageRankViewerReducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {
        @Override
        protected void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text value: values) {
                context.write(value, new DoubleWritable(-key.get()));
            }
        }
    }

}
