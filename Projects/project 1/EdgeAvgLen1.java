package comp9313.ass1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class EdgeAvgLen1 {

    /**
     * Mapper class
     */
    public static class MyMapper extends Mapper<Object, Text, IntWritable, Text>{

        private IntWritable node = new IntWritable();

//        key is node number, and value is String combined by total length and count, separated by ":"
//        generate key-value pair for each line
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                itr.nextToken();
                itr.nextToken();
                node.set(Integer.parseInt(itr.nextToken()));
                double length = Double.parseDouble(itr.nextToken());
                context.write(node,new Text(length+":"+1));
            }
        }

    }

    /**
     * Combiner class: for each mapper, sum the length and count for each key.
     */
    public static class MyCombiner extends Reducer<IntWritable, Text, IntWritable, Text> {
        private DoubleWritable result = new DoubleWritable();

        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;
            for (Text val : values) {
                String[] v = val.toString().split(":");
                sum += Double.parseDouble(v[0]);
                count += Integer.parseInt(v[1]);
            }
            context.write(key, new Text(sum+":"+count));
        }

    }

    /**
     * Reducer class
     */
    public static class MyReducer extends Reducer<IntWritable, Text, IntWritable, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        //for each key, count the average length
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;

            for (Text val : values) {
                String[] v = val.toString().split(":");
                sum += Double.parseDouble(v[0]);
                count += Integer.parseInt(v[1]);
            }
            result.set(sum/count);
            context.write(key, result);
        }

    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "EdgeAvgLen1");
        job.setJarByClass(EdgeAvgLen1.class);
        job.setMapperClass(MyMapper.class);
        job.setCombinerClass(MyCombiner.class);
        job.setReducerClass(MyReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);


    }

}
