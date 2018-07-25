package comp9313.lab2;

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
import java.util.StringTokenizer;

public class LetterCount {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());

            while (itr.hasMoreTokens()) {
//                Answer:
//                char c = itr.nextToken().toLowerCase().charAt(0);
//                if(c <= 'z' && c >= 'a'){
//                    word.set(String.valueOf(c));
//                    context.write(word, one);
//                }
                String first_letter = itr.nextToken().substring(0, 1).toLowerCase();
                if(Character.isLetter(first_letter.toCharArray()[0])){
                    word.set(first_letter);
                    context.write(word, one);
                }

            }
        }
    }


    /**
     * Combiner在mapper之后就整合数据，减少开支
     * 在这个简单的letter count问题中，combiner和reducer是一样的，但是大多数问题是不一样的。
     */
    public static class MyCombiner extends
            Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {

//            System.out.println("Reducer输入分组<" + key.toString()+">");

            int count = 0;
            for (IntWritable value : values) {
                count += value.get();
//                System.out.println("Reducer输入键值对<" + key.toString() + "," + value.get() + ">");
            }
            context.write(key, new IntWritable(count));
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
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
        Job job = Job.getInstance(conf, "letter count");
        job.setJarByClass(LetterCount.class);
        job.setMapperClass(TokenizerMapper.class);
//      添加combiner
        job.setCombinerClass(MyCombiner.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}