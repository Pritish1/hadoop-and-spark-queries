import java.io.IOException;
import java.util.StringTokenizer;

import javax.naming.Context;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AgeYearCount {

  public static class TokenizerMapper
       extends Mapper<Object, Text, IntWritable, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private IntWritable ageYear = new IntWritable();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String[] columns = value.toString().split(",");
      int k = 0;
      if(!columns[0].chars().allMatch(Character::isDigit) || !columns[20].chars().allMatch(Character::isDigit)){
         k = 0;
      } else {
         k = Integer.parseInt(columns[20]) * 10000 + Integer.parseInt(columns[0]);
      }
      ageYear.set(k);
      context.write(ageYear, one);
    }
  }

  public static class IntSumReducer
       extends Reducer<IntWritable,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();
    private Text compKey = new Text();

    public void reduce(IntWritable key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      int k = key.get();
      int age = k / 10000;
      int year = k % 10000;
      String ageYearKey = String.valueOf(age) + "\t" + String.valueOf(year);
      compKey.set(ageYearKey);
      context.write(compKey, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "ageyear_count");
    job.setJarByClass(AgeYearCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(IntSumReducer.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}