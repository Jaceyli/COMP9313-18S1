package comp9313.lab3;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

/**
 * in-mapper combining：在每个mapper中先汇总结果，然后再发给reducer汇总所有mapper。
 *
 * created by Jingxuan Li on 1/4/18
 */
public class WordCount2 {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

//        private final static IntWritable one = new IntWritable(1);
//        private Text word = new Text();
        private static HashMap<String, Integer> map = new HashMap<String, Integer>();


        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), " *$&#/\t\n\f\"'\\,.:;?![](){}<>~-_");

            while (itr.hasMoreTokens()) {
                String token = itr.nextToken().toLowerCase();
//                word.set(itr.nextToken().toLowerCase());
//                context.write(word, one);
                if(map.containsKey(token)){
                    int total = map.get(token) + 1;
                    map.put(token, total);
                }else{
                    map.put(token, 1);
                }
            }
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            Set<Map.Entry<String, Integer>> sets = map.entrySet();
            for(Map.Entry<String, Integer> entry: sets){
                String key = entry.getKey();
                int total = entry.getValue();
//                System.out.println(key+": "+total);
                context.write(new Text(key), new IntWritable(total));
            }
        }
    }


    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        FileUtils.deleteDirectory(new File("output"));

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount2.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}