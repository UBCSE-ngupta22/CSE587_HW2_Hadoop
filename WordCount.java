import java.io.IOException;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.Map;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

  public static class TokenizerMapper
      extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();


    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        String strTrimmed = itr.nextToken().replaceAll("\\p{P}","").toLowerCase();
        word.set(strTrimmed);
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

    protected void cleanup(Context context) throws IOException, InterruptedException {
        Map<Text, Integer> sortedMap = sortByValues(resultMap);
            for (Map.Entry<Text, Integer> entry : sortedMap.entrySet()) {
                context.write(entry.getKey(), new IntWritable(entry.getValue()));
            }
        }

        private Map<Text, Integer> sortByValues(Map<Text, Integer> map) {
            List<Map.Entry<Text, Integer>> list = new LinkedList<>(map.entrySet());

            list.sort((o1, o2) -> (o2.getValue()).compareTo(o1.getValue()));

            Map<Text, Integer> sortedMap = new LinkedHashMap<>();
            for (Map.Entry<Text, Integer> entry : list) {
                sortedMap.put(entry.getKey(), entry.getValue());
            }
            return sortedMap;
        }

  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}