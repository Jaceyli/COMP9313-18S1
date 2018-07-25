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

public class WordCount {

    //map class
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{
        //Object key/Text value: Data type of the input Key and Value to the mapper

        //Context: An inner class of Mapper, used to store the context of a running task.
        // Here it is used to collect data output by either the Mapper or the Reducer,
        // i.e. intermediate outputs or the output of the job

        // IntWritable: A serializable and comparable object for integer
        private final static IntWritable one = new IntWritable(1);

        //Text: stores text using standard UTF8 encoding. It provides methods to
        //serialize, deserialize, and compare texts at byte level
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            //Use a string tokenizer to split the document into words
            StringTokenizer itr = new StringTokenizer(value.toString());

            while (itr.hasMoreTokens()) {

                //Assign each work from the tokenizer(of String type) to a Text ‘word’
                word.set(itr.nextToken());
                //Form key value pairs for each word as <word, one> using context
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        //data from map will be <”word”,{1,1,..}>,
        // so we get it with an Iterator and thus we can go through the sets of values
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            //Initaize a variable ‘sum’ as 0
            int sum = 0;
            //Iterate through all the values with respect to a key and sum up all of them
            for (IntWritable val : values) {
                sum += val.get();
            }

            // Form the final key/value pairs results for each word using context
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {

        FileUtils.deleteDirectory(new File("output"));

        //Creating a Configuration object and a Job object, assigning a job name for identification purposes
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        //Setting the job's jar file by finding the provided class location
        job.setJarByClass(WordCount.class);
        //Providing the mapper and reducer class names
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        //Setting configuration object with the Data Type of output Key and Value for map and reduce
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}