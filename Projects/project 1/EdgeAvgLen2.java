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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

public class EdgeAvgLen2 {

    /**
     * Mappper class: In Mapper-combining
     */
    public static class MyMapper extends Mapper<Object, Text, IntWritable, Text>{

//        use hashmap to store node, total length and count
//        key is node number, and value is String combined by total length and count, separated by ":"

//        *should not use 'static'
        private HashMap<Integer, String> map = new HashMap<>();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            StringTokenizer itr = new StringTokenizer(value.toString());

            while (itr.hasMoreTokens()) {
                itr.nextToken();
                itr.nextToken();

//                get node number and length to the node
                int node = Integer.parseInt(itr.nextToken());
                double length = Double.parseDouble(itr.nextToken());

//              if map contains node, get the previous length of node and add the new one.
//              Then, get the counts to the node, add 1.
                if (map.containsKey(node)){
                    String[] tmp = map.get(node).split(":");
                    double total = Double.parseDouble(tmp[0]) + length;
                    map.put(node, total+":"+(Integer.parseInt(tmp[1]) + 1));

                }else {
//                  it map do not contains node, put the new length and count = 1.
                    map.put(node, length + ":" + 1);
                }

            }
        }

//        clean up the resource, store them in context
        public void cleanup(Context context) throws IOException, InterruptedException {
            Set<Map.Entry<Integer, String >> sets = map.entrySet();
            for(Map.Entry<Integer, String> entry: sets){
                int key = entry.getKey();
                String result = entry.getValue();
                context.write(new IntWritable(key), new Text(result));
            }
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
        Job job = Job.getInstance(conf, "EdgeAvgLen2");
        job.setJarByClass(EdgeAvgLen2.class);
        job.setMapperClass(MyMapper.class);
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
