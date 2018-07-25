package comp9313.ass2;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;
import java.util.*;


/**
 * =====================================================================================================
 * COMP9313 Project2 18S1
 *
 * Single target shortest path:
 * Given a graph and a node “t”, find the shortest distances of all nodes to “t” together with the paths.
 * For example, the shortest distance from node 1 to t is 7 with path 1->3->4->t.
 *
 * ----------------------------------------------------------------------------------------------------
 * The project has been separated into three parts:
 * 1.Init the input
 * 2.Find the shortest path through Dijkstra Algorithm
 * 3.Format the output
 * ----------------------------------------------------------------------------------------------------
 * Format :[Key] \t [Distance]|[Y|N]|[Adjacent list][pre_node]
 * [Key]          : The key of node
 * [Distance]     : Current short distance from the node to QueryNode
 * [Y|N]          : Y - No updated, finished
 *                  N - Updated, not finished yet
 * [Adjacent list]: Adjacent list of the node
 * [pre_node]     : The previous node of current short distance
 * For example    : 1	7.0|N|0:10.0,2:3.0|3
 * ----------------------------------------------------------------------------------------------------
 *
 * created by Jingxuan Li on 18/4/18
 * =====================================================================================================
 */
public class SingleTargetSP {


    public static String OUT = "output";
    public static String IN = "input";
    public static String QueryNodeId;

    public enum COUNTER{   //Set a counter
        UPDATE_FLAG
    }

//-----------------------------------------------------------------------------------------------
// 1. Init the input
//-----------------------------------------------------------------------------------------------
    /**
     *  InitMapper:
     *  In order to find the shortest distances of all nodes to “t” (no "t" to all nodes),
     *  I swap the nodeFrom and nodeTo.
     *
     *  For one special case:
     *      The nodes only get the outer connections, after swaping these node are not key anymore
     *      To solve it, for each line, emit (nodeFrom,"").
     *      In reducer, it will not effect other keys;
     *      for these special keys, generate the result without adjacent node list, like (key,"-1.0|N||N").
     */
    public static class InitMapper extends Mapper<Object, Text, LongWritable, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String token[] = line.split(" ");
            context.write(new LongWritable(Long.parseLong(token[2])), new Text(token[1] + ":" + token[3]+","));
            context.write(new LongWritable(Long.parseLong(token[1])), new Text(""));
        }
    }

    /**
     *  InitReducer:
     *  Aggregate the adjacent node list, and format result
     */
    public static class InitReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

        @Override
        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String result = "";
            for (Text val : values) {
//               get the adjacent node list
                result += val;
            }
            if(result.equals("")){
                result = "-1.0|N||N";
                context.write(key, new Text(result));
            }else {
                result = result.substring(0, result.length() - 1);
                if (key.toString().equals(QueryNodeId)) {
//                if the node is querynode, then the distance is 0
                    result = "0.0|N|" + result + "|N";
                } else {
//                "-1.0" represents there is no shortest path yet. (Infinite)
                    result = "-1.0|N|" + result + "|N";
                }
                context.write(key, new Text(result));
            }

        }
    }


//-----------------------------------------------------------------------------------------------
// 2.Find the shortest path through Dijkstra Algorithm
//-----------------------------------------------------------------------------------------------
    /**
     * SingleSourceMapper:
     * Emit: distance from adjacency list and the current distance
     */
    public static class SingleSourceMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String node = value.toString().split("\t")[0];
            String adjacency_list = value.toString().split("\t")[1];
            String token[] = adjacency_list.split("\\|");
            Double distance = Double.parseDouble(token[0]);
            String flag = token[1];
            String pre_node = token[3];

//           if current distance is not Infinite, then emit distance from adjacency list
            if(!token[2].equals("")) {
                if (distance >= 0 && flag.equals("N")) {
                    if (token[2].contains(",")) {
                        String pairs[] = token[2].split(",");
                        for (String pair1 : pairs) {
                            String pair[] = pair1.split(":");
                            context.write(new LongWritable(Long.parseLong(pair[0])), new Text(String.valueOf((Double.parseDouble(pair[1]) + distance)) + "#" + node));
//                            System.out.println(pair[0] + "," + pair[1]);
                        }
                    } else {
                        String pair[] = token[2].split(":");
                        context.write(new LongWritable(Long.parseLong(pair[0])), new Text(String.valueOf((Double.parseDouble(pair[1]) + distance)) + "#" + node));
//                        System.out.println(pair[0] + "," + pair[1]);
                    }
                }
            }
    //            emit the current distance
            context.write(new LongWritable(Long.parseLong(node)), new Text(adjacency_list + "|\t" + pre_node));
//            System.out.println(node + "," + adjacency_list);
        }
    }


    /**
     * SingleSourceReducer:
     * Selects minimum distance path for each reachable node
     */
    public static class SingleSourceReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

        @Override
        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            Double cur_distance = -1.0;
            Double min_distance = Double.POSITIVE_INFINITY;
            String adjacency_list = "";
            String pre_node = "";
            String new_pre_node = "";

            for(Text val: values){
                String value = val.toString();
                if(value.contains("|")){
//                  get current short distance
                    String token[] = value.split("\\|");
                    cur_distance = Double.parseDouble(token[0]);
                    adjacency_list = token[2];
                    pre_node = token[3];
                }else{
//                    get the min distance
                    String tokens[] = val.toString().split("#");
                    new_pre_node = tokens[1];
                    Double tmp = Double.parseDouble(tokens[0]);
                    if(tmp < min_distance){
                        min_distance = tmp;
                    }
                }
            }
            if((cur_distance == -1 || min_distance < cur_distance) && min_distance != Double.POSITIVE_INFINITY){
//                if find shorter distance, update the shorter distance and previous node.
//                Flag: N and Count++
                context.write(key, new Text(min_distance + "|N|" + adjacency_list + "|" + new_pre_node));
//                System.out.println(key+"\t"+min_distance+"|N|"+ adjacency_list );
                context.getCounter(COUNTER.UPDATE_FLAG).increment(1);
            }else {
//                if do not find shorter distance, no update. Flag turns to Y.
                context.write(key, new Text(cur_distance+"|Y|"+ adjacency_list + "|" + pre_node));
//                System.out.println(key+"\t"+cur_distance+"|Y|"+ adjacency_list + "|" + pre_node);
            }
        }
    }


//-----------------------------------------------------------------------------------------------
//    Format the output
//-----------------------------------------------------------------------------------------------

    public static class OutMapper extends Mapper<Object, Text, LongWritable, Text> {

        private static HashMap<String, String> path_map = new HashMap<>();
        private static HashMap<String, String> distance_map = new HashMap<>();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String node = value.toString().split("\t")[0];
            String tmp = value.toString().split("\t")[1];
            String token[] = tmp.split("\\|");
            String pre_node = token[3];
            String path = node + "->" + pre_node;

            if (!token[0].equals("-1.0")){
//                if the node can reach the query node, get the path and distance
                path_map.put(node, path);
                distance_map.put(node, token[0]);
            }
        }

        /**
         * Generate the whole path of each node
         */
        public void cleanup(Context context) throws IOException, InterruptedException {
            Set<Map.Entry<String, String>> sets = path_map.entrySet();
            for(Map.Entry<String, String> entry: sets){
                String key = entry.getKey();
                String path = entry.getValue();
                String nodes[] = path.split("->");
                String last_node = nodes[nodes.length - 1];

//              To generate the path , we keep looking the previous node until reach the query node.
                while(!last_node.equals(QueryNodeId) && !last_node.equals("N")){
                    String tmp_path = path_map.get(last_node);
                    String tmp_nodes[] = tmp_path.split("->");
                    last_node = tmp_nodes[tmp_nodes.length - 1];
                    String add_path = tmp_path.substring(tmp_path.indexOf("->"), tmp_path.length());
                    path = path + add_path;
                }
                path_map.put(key, path);
                context.write(new LongWritable(Long.parseLong(key)), new Text(distance_map.get(key) + "\t" + path));
            }
        }
    }


    public static class OutReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

        @Override
        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text val:values){
                if(key.toString().equals(QueryNodeId)){
                    context.write(key, new Text("0.0\t" + key));
                }else{
                    context.write(key, val);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {

        IN = args[0];
        OUT = args[1];
        QueryNodeId = args[2];

        int iteration = 0;
        String input = IN;
        String output = OUT + iteration;

        Configuration conf = new Configuration();

        //Run the first MapReduce
        Job job1 = Job.getInstance(conf, "Init Input");
        job1.setJarByClass(SingleTargetSP.class);
        job1.setMapperClass(InitMapper.class);
        job1.setReducerClass(InitReducer.class);
        job1.setOutputKeyClass(LongWritable.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(input));
        FileOutputFormat.setOutputPath(job1, new Path(output));

        job1.waitForCompletion(true);


        //Run the second MapReduce
        boolean isdone = false;

        while (!isdone) {

            input = output;
            iteration ++;
            output = OUT + iteration;

            Job job2 = Job.getInstance(conf, "Single Source SP");
            job2.setJarByClass(SingleTargetSP.class);
            job2.setMapperClass(SingleSourceMapper.class);
            job2.setReducerClass(SingleSourceReducer.class);
            job2.setOutputKeyClass(LongWritable.class);
            job2.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job2, new Path(input));
            FileOutputFormat.setOutputPath(job2, new Path(output));

            job2.waitForCompletion(true);

            Counters counters = job2.getCounters();
            long updates = counters.findCounter(COUNTER.UPDATE_FLAG).getValue();
//            System.out.println("Updates: " + updates);

            if (updates == 0)
                isdone = true;

        }

        //Run the third MapReduce
        input = output;
//        iteration ++;
        output = OUT + "final_result";

        Job job3 = Job.getInstance(conf, "Format Output");
        job3.setJarByClass(SingleTargetSP.class);
        job3.setMapperClass(OutMapper.class);
        job3.setReducerClass(OutReducer.class);
        job3.setOutputKeyClass(LongWritable.class);
        job3.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job3, new Path(input));
        FileOutputFormat.setOutputPath(job3, new Path(output));

        job3.waitForCompletion(true);
    }
}

