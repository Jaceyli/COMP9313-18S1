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
import java.util.ArrayList;
import java.util.StringTokenizer;

/**
 * The problem is to compute the number of co-occurrence for each pair of terms (w, u) in the document.
 * In this problem, the co-occurrence of (w, u) is defined as: u appears after w in a line of document.
 * This means that, the co- occurrence counts of (w, u) and (u, w) are different!
 * The task is to use the “pair” approach to solve this problem.
 *
 * created by Jingxuan Li on 16/4/18
 */

public class CoTermNSPair {

    public static class PairMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text pair = new Text();


        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), " *$&#/\t\n\f\"'\\,.:;?![](){}<>~-_");

            //把每一行的单词都放入ArrayList中
            ArrayList<String> arrayList = new ArrayList<String>();
            while (itr.hasMoreTokens()) {
                arrayList.add(itr.nextToken().toLowerCase());
                }
//            遍历每个单词，与其后面所有的单词组成pair，存在context中
            for(int i = 0; i < arrayList.size(); i++){
                for(int j = i + 1; j < arrayList.size(); j++){
                    pair.set(arrayList.get(i) + " " +arrayList.get(j));
                    context.write(pair, one);
//                    System.out.println(pair);
                }
            }
        }

    }

    public static class PairReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
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
        Job job = Job.getInstance(conf, "CoTermNSPair");
        job.setJarByClass(CoTermNSPair.class);
        job.setMapperClass(PairMapper.class);
//        Combiner和reducer相同的情况下，直接调用reducer就可以
        job.setCombinerClass(PairReducer.class);
        job.setReducerClass(PairReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}