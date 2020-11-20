import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import java.io.*;
import java.math.BigDecimal;
import java.util.*;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import org.ansj.app.summary.SummaryComputer;
import org.ansj.domain.Result;
import org.ansj.domain.Term;
import org.ansj.library.DicLibrary;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.ansj.splitWord.analysis.DicAnalysis;
import org.apache.hadoop.util.GenericOptionsParser;
import org.nlpcn.commons.lang.tire.domain.Forest;
import org.nlpcn.commons.lang.tire.domain.Value;
import org.nlpcn.commons.lang.tire.library.Library;

import javax.swing.plaf.metal.OceanTheme;


public class DataPreprocessing {

  //task1 分词
  public static class ProMapper extends Mapper<Object, Text, Text, Text> {
    @Override
    public void setup(Context context)
    {
      String[] names={"哑巴","汉子","大汉","老头子","说不得","老不死","车夫","胖子","铁匠","老王","童子","农夫","何足道","明月","少妇","脚夫","皇太后","清风","室里","胖妇人"};
      HashMap<String,Integer> hashMap=new HashMap<String, Integer>();
      for(String name:names)
        hashMap.put(name,1);
      String nameList = context.getConfiguration().get("OurNameList");
      String[] strs = nameList.split("\t");
      for(String str : strs) {
        if(!hashMap.containsKey(str))
          DicLibrary.insert(DicLibrary.DEFAULT, str);
      }
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      FileSplit fileSplit = (FileSplit)context.getInputSplit();
      StringTokenizer izer = new StringTokenizer(value.toString());
      String buf = "";
      List<Term> terms = DicAnalysis.parse(value.toString()).getTerms();
      for(Term term : terms)
      {
        if(term.getNatureStr().equals("userDefine"))
        {
          buf = buf + "\t" + term.getName();
        }

      }
      context.write(new Text(buf), new Text("NULL"));
    }
  }

  public static class ProReducer extends Reducer<Text,Text,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      context.write(key, new Text(""));
    }
  }

  //task2 统计同现
  public static class CoMapper extends Mapper<Object, Text, Text, Text> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      FileSplit fileSplit = (FileSplit)context.getInputSplit();
      String[] names = value.toString().split("\t");
    //  List<String> namelist = Arrays.asList(names);
    //  List<String> arrList = new ArrayList<String>(namelist); //
      HashMap<String, Integer> hashMap = new HashMap<String, Integer>();

      for(int i=0;i<names.length;i++)
      {
        if(names[i]==null||names[i].equals(""))
          continue;
        for(int j=0;j<names.length;j++)
        {
          if(i==j||names[j]==null||names[j].equals(""))
            continue;
          if(names[i].equals(names[j])) {
            names[j] = null;continue;
          }
            hashMap.put(names[i] + "," + names[j], 1);
        }
      }


      Iterator iter = hashMap.entrySet().iterator();
      while (iter.hasNext()) {
        Map.Entry entry = (Map.Entry) iter.next();
        String text = (String) entry.getKey();
        context.write(new Text(text), new Text("1"));
      }
    }
  }

  public static class CoReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      int i = 0;
      for(Text value: values)
        i++;
      context.write(key,new Text(Integer.toString(i)));
    }
  }

  static String readHDFSFile(String filePath)  throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(URI.create(filePath), conf);
    InputStream in = fs.open(new Path(filePath));
    StringBuilder sb = new StringBuilder();
    String line;
    BufferedReader br = new BufferedReader(new InputStreamReader(in));
    while ((line = br.readLine()) != null) {
      sb.append(line+"\t");
    }
    String str = sb.toString();
    return str;
  }

  static String readNameist(String path) throws FileNotFoundException, UnsupportedEncodingException, IOException ///不能用来读hdfs文件
  {
    File file = new File(path);
    StringBuilder result = new StringBuilder();
    try
    {
      BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF-8"));

      String s = null;
      while((s = br.readLine())!=null){
        result.append(s+"\t");
      }
      br.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
    return result.toString();
  }

  static void setUserDefineLibrary(String list)
  {
    String[] strs = list.split("\t");
    for(String str : strs)
    {
      System.out.println(str);
      DicLibrary.insert(DicLibrary.DEFAULT,str);
    }
  }

  //task3 normalization
  public static class NoMapper extends Mapper<Object, Text, Text, Text> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      FileSplit fileSplit = (FileSplit)context.getInputSplit();
      StringTokenizer izer = new StringTokenizer(value.toString());
      String k=izer.nextToken();
      String v=izer.nextToken();
      String []names= k.split(",");
      context.write(new Text(names[0]),new Text(names[1]+","+v));
    }
  }

  public static class NoReducer extends Reducer<Text,Text,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      String name1=key.toString();
      Vector<String> name2s=new Vector<String>();
      Vector<Integer> weights=new Vector<Integer>();
      int sum=0;
      for(Text text:values) {
        String []t=text.toString().split(",");
        name2s.add(t[0]);
        int weight=Integer.parseInt(t[1]);
        sum+=weight;
        weights.add(weight);
      }
      String buf="[";
      for(int i=0;i<name2s.size();i++)
      {
        BigDecimal num=new BigDecimal((double)weights.get(i)/(double)sum);
        buf=buf+name2s.get(i)+","+String.valueOf(num.setScale(5,BigDecimal.ROUND_HALF_UP))+"|";
      }
      StringBuilder strBuilder = new StringBuilder(buf);
      strBuilder.setCharAt(buf.length()-1, ']');
      context.write(key, new Text(strBuilder.toString()));
    }
  }



  public static void main(String[] args) throws Exception {
    Configuration config = new Configuration();
    String[] remainArgs = new GenericOptionsParser(config, args).getRemainingArgs();

    String job1InputPath = remainArgs[0];
    String job1InputPath2 = remainArgs[1];
    String job1OutputPath = remainArgs[2];

    //job1 分词
    Job job1 = Job.getInstance(config, "Data Preprocessing_2");
    job1.setJarByClass(DataPreprocessing.class);
    job1.setMapperClass(ProMapper.class);
    job1.setReducerClass(ProReducer.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job1, new Path(job1InputPath));
    String nameList = readHDFSFile(job1InputPath2);
    job1.getConfiguration().set("OurNameList",nameList);
    FileOutputFormat.setOutputPath(job1, new Path(job1OutputPath));

    //job2同现
    String job2InputPath = job1OutputPath;
    String job2OutputPath = remainArgs[3];

    Job job2 = Job.getInstance(config, "Co-occurrence");
    job2.setJarByClass(DataPreprocessing.class);
    job2.setMapperClass(CoMapper.class);
    job2.setReducerClass(CoReducer.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job2, new Path(job2InputPath));
    FileOutputFormat.setOutputPath(job2, new Path(job2OutputPath));

    //job3归一化
    String job3InputPath = job2OutputPath;
    String job3OutputPath = remainArgs[4];
    Job job3 = Job.getInstance(config, "Normalization2");
    job3.setJarByClass(DataPreprocessing.class);
    job3.setMapperClass(NoMapper.class);
    job3.setReducerClass(NoReducer.class);
    job3.setOutputKeyClass(Text.class);
    job3.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job3, new Path(job3InputPath));
    FileOutputFormat.setOutputPath(job3, new Path(job3OutputPath));




    if (job1.waitForCompletion(true)) {
      if(job2.waitForCompletion(true))
          System.exit(job3.waitForCompletion(true) ? 0 : 1);
    }


  }


}

