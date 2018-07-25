import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.util.*;

public class SetSimJoin {

    // self class, use for sort in the final map-reduce
    public static class IntPair implements WritableComparable<IntPair> {
        private int first, second;

        public IntPair() {
        }

        public IntPair(int first, int second) {
            set(first, second);
        }

        public void set(int left, int right) {
            first = left;
            second = right;
        }

        public int getFirst() {
            return first;
        }

        public int getSecond() {
            return second;
        }

        public void write(DataOutput out) throws IOException {
            out.writeInt(first);
            out.writeInt(second);
        }

        public int compareTo(IntPair o) {
            if (first != o.first) {
                return first < o.first ? -1 : 1;
            } else if (second != o.second) {
                return second < o.second ? -1 : 1;
            } else {
                return 0;
            }
        }

        public boolean equals(Object right) {
            if (right == null)
                return false;
            if (this == right)
                return true;
            if (right instanceof IntPair) {
                IntPair r = (IntPair) right;
                return r.first == first && r.second == second;
            } else {
                return false;
            }
        }

        public void readFields(DataInput in) throws IOException {
            first = in.readInt();
            second = in.readInt();
        }
    }

    // input: <rid eid eid ..>
    // threshold: t
    // emit: <eid, rid eid eid ..>
    // filter: Prefix Filter: p = len(l) - upperBound(t * len(l)) + 1
    //
    public static class SSJoinMapper extends Mapper<Object, Text, IntWritable, Text> {
        private IntWritable elementId = new IntWritable();
        private Text recordIdElementIds = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            Configuration configuration = context.getConfiguration();
            Double threshold = Double.valueOf(configuration.get("threshold"));

            StringTokenizer itr = new StringTokenizer(value.toString(), "\n");

            while (itr.hasMoreTokens()) {
                // line format: rid eid eid ..
                String line = itr.nextToken();
                String[] tokens = line.split(" ");

                recordIdElementIds.set(line);

                int length = tokens.length - 1;
                int prefixLength = length - (int) Math.ceil(length * threshold) + 1;
//                System.out.println("prefixLength: " + prefixLength);

                for (int i = 1; i < prefixLength + 1; i++) {
                    elementId.set(Integer.valueOf(tokens[i]));
                    context.write(elementId, recordIdElementIds);
                    System.out.println("map: " + elementId.toString() + "\t" + recordIdElementIds.toString());
                }
            }
        }
    }

    // input: <eid, rid eid eid ..>
    // threshold: t
    // output: <, rid rid sim>
    public static class SSJoinReducer extends Reducer<IntWritable, Text, Text, Text> {
        private Text recordIdPairSimilarity = new Text();

        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            Configuration configuration = context.getConfiguration();
            Double threshold = Double.valueOf(configuration.get("threshold"));

            HashMap<Integer, Set<Integer>> records = new HashMap<Integer, Set<Integer>>();
            for (Text value : values) {
                String[] recordIdElementIds = value.toString().split(" ");

                int rid = Integer.valueOf(recordIdElementIds[0]);
                Set<Integer> eids = new HashSet<Integer>();
                for (int i = 1; i < recordIdElementIds.length; i++) {
                    eids.add(Integer.valueOf(recordIdElementIds[i]));
                }
                records.put(rid, eids);
                System.out.println("r:"+records);
            }

            System.out.println("all"+records);

            for (int rid1 : records.keySet()) {
                for (int rid2 : records.keySet()) {
                    if (rid1 < rid2) {
                        Set<Integer> eids1 = records.get(rid1);
                        Set<Integer> eids2 = records.get(rid2);

                        Set<Integer> allids = new HashSet<Integer>();
                        allids.addAll(eids1);
                        allids.addAll(eids2);

                        Double sim = ((double) eids1.size() + (double) eids2.size() - (double) allids.size()) / (double) allids.size();
                        System.out.println(rid1+" "+rid2+" "+sim);

                        if (sim >= threshold) {
                            recordIdPairSimilarity.set(rid1 + " " + rid2 + " " + sim);
                            context.write(null, recordIdPairSimilarity);
//                            System.out.println("reduce: " + recordIdPairSimilarity.toString());
                        }
                    }
                }
            }
        }
    }

    // format mapper
    // input: <rid1 rid2 sim>
    // emit: <ridpair, sim>
    public static class FormatMapper extends Mapper<Object, Text, IntPair, DoubleWritable> {
        private IntPair recordIdPair = new IntPair();
        private DoubleWritable similarity = new DoubleWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            StringTokenizer itr = new StringTokenizer(value.toString(), "\n");

            while (itr.hasMoreTokens()) {
                // line format: rid eid eid ..
                String line = itr.nextToken();
                String[] tokens = line.split(" ");

                int rid1 = Integer.valueOf(tokens[0]);
                int rid2 = Integer.valueOf(tokens[1]);
                double sim = Double.valueOf(tokens[2]);

                recordIdPair.set(rid1, rid2);
                similarity.set(sim);

                context.write(recordIdPair, similarity);
                System.out.println("map: " + recordIdPair.getFirst() + " " + recordIdPair.getSecond() + " " + similarity.toString());
            }

        }
    }

    // input: <ridpair, sim>
    // output: <, (rid1,rid2)'\t'sim>
    public static class FormatReducer extends Reducer<IntPair, DoubleWritable, Text, Text> {
        private Text recordIdPairSimilarity = new Text();

        public void reduce(IntPair key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

            for (DoubleWritable value : values) {
                recordIdPairSimilarity.set("(" + key.getFirst() + "," + key.getSecond() + ")\t" + value);
            }
            context.write(null, recordIdPairSimilarity);
//            System.out.println("reduce: " + recordIdPairSimilarity.toString());
        }
    }


    public static void main(String[] args) throws Exception {

        String input = args[0];
        String output = args[1];

        String threshold = args[2];
        int numOfReducers = Integer.valueOf(args[3]);

        // stage 1: sort tokens by frequency is already done by Mr. Xin Cao
        //
        // stage 2: naive => SSJoin, Prefix Filtering
        Configuration configuration = new Configuration();
        configuration.set("threshold", threshold);

        Job job = Job.getInstance(configuration, "SSJoin Prefix Filtering");
        job.setJarByClass(SetSimJoin.class);
        job.setMapperClass(SSJoinMapper.class);
        job.setReducerClass(SSJoinReducer.class);
        job.setNumReduceTasks(numOfReducers);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        String tempOutput = output + System.nanoTime();
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(tempOutput));
        job.waitForCompletion(true);


        // stage 3: sort and remove duplicates
        //
        Configuration configuration3 = new Configuration();

        Job job3 = Job.getInstance(configuration3, "sort and remove duplicates");
        job3.setJarByClass(SetSimJoin.class);
        job3.setMapperClass(FormatMapper.class);
        job3.setReducerClass(FormatReducer.class);
        job3.setNumReduceTasks(numOfReducers);

        job3.setMapOutputKeyClass(IntPair.class);
        job3.setMapOutputValueClass(DoubleWritable.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job3, new Path(tempOutput));
        FileOutputFormat.setOutputPath(job3, new Path(output+'1'));
        job3.waitForCompletion(true);

        // You can consider to delete the output folder in the previous
        // iteration to save disk space.
//        FileSystem hdfs = FileSystem.get(new URI(output), configuration3);
//        Path tmpPath = new Path(tempOutput);
//
//        if (hdfs.exists(tmpPath)) {
//            hdfs.delete(tmpPath, true);
//        }
    }
}