package comp9313.lab4;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

/**
 * Order Inversion
 *
 * The problem is to compute the relative frequency f(wj|wi)
 * Here, N(., .) indicates the number of times a particular co-occurring term pair is observed in the corpus.
 * We need the count of the term co-occurrence, divided by the marginal (the sum of the counts of the conditioning variable co-occurring with anything else).
 * In this problem, we consider the nonsymmetric co-occurrence. That is, wi and wj co-occur if wj appears after wi in the same line, which is defined the same as in Problem 2 of Lab 3.
 * Your output should be in format of (wi, wj, f(wj|wi)),
 * and you need to use DoubleWritable to serialize the value f(wj|wi).
 *
 * created by Jingxuan Li on 16/4/18
 */
public class NSRelativeFreq {
    public static class RelativeFreqMapper extends Mapper<Object, Text, Text, IntWritable> {

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

                    //*: 用于计算每个单词的总数
                    pair.set(arrayList.get(i) + " *");
                    context.write(pair, one);
                }
            }
        }

    }

    /**
     * Combiner
     *
     * 在combiner中 * 就算好和了
     */
    public static class RelativeFreqCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
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

//    /**
//     * Partitioner
//     *
//     * If you use more than 1 reducer, then you need to write a partitioner to guarantee that
//     * all key-value pairs relevant to the first term are sent to the same reducer!
//   */
//    public static class RelativeFreqPartitioner extends Partitioner<Text ,IntWritable>{
//
//        public int getPartition(Text key, IntWritable value, int numPartitions) {
//
//            //get the first term, and compute the hash value based on it
//            String firstWord = key.toString().split(" ")[0];
//            return (firstWord.hashCode() & Integer.MAX_VALUE) % numPartitions;
//        }
//    }

    /**
     * Reducer
     *
     * The key containing "*" will always arrive at the reducer first
     */
    public static class RelativeFreqReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

        private DoubleWritable result = new DoubleWritable();
        private double current_marginal = 0;

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            if(key.toString().contains("*")){
//               key有 * 的就是marginal
                current_marginal = sum;
//                System.out.println("\n------"+key.toString() +" :" + current_marginal);
            }else {
//                没有* 就 / marginal 就是概率
                result.set(sum / current_marginal);
                context.write(key, result);
//                System.out.println(key.toString()+":"+ sum + "/"+ current_marginal);
            }

        }
    }

    public static void main(String[] args) throws Exception {
        FileUtils.deleteDirectory(new File("output"));

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "NSRelativeFreq");
        job.setJarByClass(NSRelativeFreq.class);
        job.setMapperClass(RelativeFreqMapper.class);
        job.setCombinerClass(RelativeFreqCombiner.class);
//        job.setPartitionerClass(RelativeFreqPartitioner.class);
        job.setReducerClass(RelativeFreqReducer.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}


//这里pair用的是Text。
//根据MapReduce对key排序，key（word1 *）自动排到key（word1 word2）前面
