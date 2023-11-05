import java.io.IOException;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.Map;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountExt {

  public static class TokenizerMapper
      extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

      StringTokenizer itr = new StringTokenizer(value.toString());

      String stopWordsReg = "\\b(?:a|an|the|in|my|has|as|if|do|have|had|on|at|of|for|by|with|to|up|down|and|or|not|but|is|am|are|was|were|be|being|been|it|this|that|these|those|I|me|myself|we|us|our|ours|you|your|yours|he|him|his|she|her|hers|it's|its|they|them|their|theirs|what|which|who|whom|whose|here|there|when|where|why|how|all|any|both|each|few|more|most|other|some|such|no|nor|not|only|own|same|so|than|too|very|s|t|can|will|just|don|should|now)\\b";

      while (itr.hasMoreTokens()) {
        String strTrimmed = itr.nextToken().replaceAll("\\p{P}","").toLowerCase();
        Pattern pattern = Pattern.compile(stopWordsReg, Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(strTrimmed);
        String resultStr = matcher.replaceAll("");
        resultStr = resultStr.replaceAll("\\s+", " ").trim();
        word.set(resultStr);
        context.write(word, one);
      }
    }
  }

  public static class IntSumReducer
      extends Reducer<Text, IntWritable, Text, IntWritable> {
        
    private HashMap<Text, Integer> resultMap = new HashMap<>();

    public void reduce(Text key, Iterable<IntWritable> values,
        Context context) throws IOException, InterruptedException {
      
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      resultMap.put(new Text(key), sum);

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      List<Map.Entry<Text, Integer>> list = new LinkedList<>(resultMap.entrySet());

      list.sort((o1, o2) -> o2.getValue().compareTo(o1.getValue()));

      if (!list.isEmpty()) {
          list.remove(0);
      }
      
      int count = 0;
      for (Map.Entry<Text, Integer> entry : list) {
          context.write(entry.getKey(), new IntWritable(entry.getValue()));
          count++;
          if(count == 25){
            break;
          }
      }
    }

  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count Extended");
    job.setJarByClass(WordCountExt.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}