package comp9313.lab3;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;


/**
 * The problem is to compute the number of co-occurrence for each pair of terms (w, u) in the document.
 * In this problem, the co-occurrence of (w, u) is defined as: u appears after w in a line of document.
 * This means that, the co- occurrence counts of (w, u) and (u, w) are different!
 * The task is to use the “stripe” approach to solve this problem.
 *
 * created by Jingxuan Li on 16/4/18
 */
public class CoTermNSStripe {
    public static class StripeMapper extends Mapper<Object, Text, Text, MapWritable>{

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            StringTokenizer itr = new StringTokenizer(value.toString(), " *$&#/\t\n\f\"'\\,.:;?![](){}<>~-_");

            ArrayList<String> arrayList = new ArrayList<String>();
            while(itr.hasMoreTokens()){
                arrayList.add(itr.nextToken().toLowerCase());
            }

            for(int i = 0; i < arrayList.size(); i++){
                MapWritable record = new MapWritable();
                Text word1 = new Text(arrayList.get(i));
                for(int j = i + 1; j < arrayList.size(); j++){
                    Text word2 = new Text(arrayList.get(j));
                    //a → { b: 1, c: 2, d: 5, e: 3, f: 2 }
                    //如果word2在record中结果中（例如{ b: 1, c: 2, d: 5, e: 3, f: 2 }），获取原来的值+1
                    if (record.containsKey(word2)){
                        IntWritable count = (IntWritable) record.get(word2);
                        count.set(count.get() + 1);
                        record.put(word2, count);
                    }else{
                        //如果不在record中就 写入 word2 : 1
                        record.put(word2,new IntWritable(1));
                    }
                }
                context.write(word1,record);
            }
        }
    }


    public static class StripeCombiner extends Reducer<Text, MapWritable, Text, MapWritable> {
        public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException{

            MapWritable record = new MapWritable();
            for(MapWritable val : values){
                Set<Entry<Writable, Writable>> sets = val.entrySet();
                for(Entry<Writable, Writable> entry: sets){
                    Text word2 = (Text)entry.getKey();
                    int count = ((IntWritable)entry.getValue()).get();
                    if (record.containsKey(word2)){
                        count += ((IntWritable)record.get(word2)).get();
                    }
                    record.put(word2, new IntWritable(count));
                }
            }
            context.write(key, record);
        }
    }

    public static class StripeReducer extends Reducer<Text, MapWritable, Text, IntWritable> {

        private Text word = new Text();
        private IntWritable count = new IntWritable();

        public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException{

            //注意map不是全局变量
            HashMap<String, Integer> map = new HashMap<String, Integer>();

            for (MapWritable val : values) {
                Set<Entry<Writable, Writable>> sets = val.entrySet();
                for(Entry<Writable, Writable> entry: sets){
                    String word2 = entry.getKey().toString();
                    int sum = ((IntWritable)entry.getValue()).get();
                    if (map.containsKey(word2)) {
                        sum += map.get(word2);
                    }
                    map.put(word2, sum);
                }
            }
//            System.out.println(map);

            // 排序
            Object[] sortedkey = map.keySet().toArray();
            Arrays.sort(sortedkey);
            for (int i = 0; i < sortedkey.length; i++) {
                word.set(key.toString() + " " + sortedkey[i].toString());
                count.set(map.get(sortedkey[i]));
//                System.out.println(word.toString()+" "+count.toString());
                context.write(word, count);
            }

        }

    }

    public static void main(String[] args) throws Exception {
        FileUtils.deleteDirectory(new File("output"));

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "CoTermNSPair");
        job.setJarByClass(CoTermNSStripe.class);
        job.setMapperClass(StripeMapper.class);
        job.setCombinerClass(StripeCombiner.class);
        job.setReducerClass(StripeReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MapWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
