// Code Author:-
// Name: Shivam Gupta
// Net ID: SXG190040
// CS 6350.001 - Big Data Management and Analytics - F20 Assignment 1 (Hadoop MapReduce)


import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;



public class Maximum_Friends_Pair {
    public static class MapperClass1 extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text values, Context Data) throws IOException, InterruptedException {

            String[] Friends_List = values.toString().split("\t");

            if (Friends_List.length == 2) {

                int friend1_ID = Integer.parseInt(Friends_List[0]);
                String[] Friends_IDs_List = Friends_List[1].split(",");
                int friend2_ID;
                Text friends_Key = new Text();
                for (String friend2 : Friends_IDs_List) {
                    friend2_ID = Integer.parseInt(friend2);
                    if (friend1_ID < friend2_ID) {
                        friends_Key.set(friend1_ID + "," + friend2_ID);
                    } else {
                        friends_Key.set(friend2_ID + "," + friend1_ID);
                    }
                    Data.write(friends_Key, new Text(Friends_List[1]));
                }
            }
        }
    }

    public static class ReducerClass1 extends Reducer<Text, Text, Text, IntWritable> {

        public void reduce(Text key, Iterable<Text> values, Context Data) throws IOException, InterruptedException {
            HashMap<String, Integer> Hash_Map_friends = new HashMap<String, Integer>();
            //System.out.println("Key " +key);
            int Mutual_Friends_Count = 0;
            for (Text Friends_Tuple : values) {
                String[] Friends_IDs_List = Friends_Tuple.toString().split(",");
                for (String Every_Frnd_ID : Friends_IDs_List) {
                    if (Hash_Map_friends.containsKey(Every_Frnd_ID)) {
                        Mutual_Friends_Count++;
                    } else {
                        Hash_Map_friends.put(Every_Frnd_ID, 1);
                    }
                }
            }
            Data.write(key, new IntWritable(Mutual_Friends_Count));
        }
    }

    public static class MapperClass2 extends Mapper<Text, Text, LongWritable, Text> {

        private LongWritable Frnd_Count = new LongWritable();

        public void map(Text key, Text values, Context Data) throws IOException, InterruptedException {
            int mut_count = Integer.parseInt(values.toString());
            Frnd_Count.set(mut_count);
            Data.write(Frnd_Count, key);
            // System.out.println(Data.getCurrentKey()+ " " + Data.getCurrentValue());
        }
    }

    public static class ReducerClass2 extends Reducer<LongWritable, Text, Text, LongWritable> {
        private int INDEX = 0;
        private Long MAX_Mutual_Count = 0L;

        public void reduce(LongWritable key, Iterable<Text> values, Context Data)
                throws IOException, InterruptedException {
            // For writing only the Maximum Count Friends Pairs
            if (INDEX == 0) {
                MAX_Mutual_Count = key.get();
                for (Text VAL : values) {
                    Data.write(VAL, key);
//                    System.out.println("Maximum Friends   " + Data.getCurrentKey() + " " + Data.getCurrentValue());
                }

                INDEX++;
            }
        }
    }
    
    // Main Function for the Driver MApReduce Code for Maximum Common Friends
    public static void main(String[] Arguments) throws Exception {
        // Taking all the required Arguments
        if (Arguments.length != 3) {
            System.out.println("Please Give Arguments in the Valid Format: <soc_input_file > <intermediate_file_path> <output_path>");
            System.exit(1);
        }

        {
            // Creating a job with name "Mutual Friends"
            Configuration Config_Job_1 = new Configuration();
            Job JOB_max_frnd_1 = Job.getInstance(Config_Job_1, "Mutual Friends");

            // setting the type of output keys and Values
            JOB_max_frnd_1.setJarByClass(Maximum_Friends_Pair.class);
            JOB_max_frnd_1.setMapperClass(MapperClass1.class);
            JOB_max_frnd_1.setReducerClass(ReducerClass1.class);

            JOB_max_frnd_1.setMapOutputKeyClass(Text.class);
            JOB_max_frnd_1.setMapOutputValueClass(Text.class);

            JOB_max_frnd_1.setOutputKeyClass(Text.class);
            JOB_max_frnd_1.setOutputValueClass(IntWritable.class);

            // setting the HDFS path for the Result Text Data
            FileInputFormat.addInputPath(JOB_max_frnd_1, new Path(Arguments[0]));
            FileOutputFormat.setOutputPath(JOB_max_frnd_1, new Path(Arguments[1]));

            if (!JOB_max_frnd_1.waitForCompletion(true)) {
                System.exit(1);
            }

            {
                // Creating a job with name "Maximum Common Friends"
                Configuration Config_Job_2 = new Configuration();
                Job JOB_max_frnd_2 = Job.getInstance(Config_Job_2, "Maximum Common Friends");

                // setting the type of output keys and Values

                JOB_max_frnd_2.setJarByClass(Maximum_Friends_Pair.class);
                JOB_max_frnd_2.setMapperClass(MapperClass2.class);
                JOB_max_frnd_2.setReducerClass(ReducerClass2.class);

                JOB_max_frnd_2.setMapOutputKeyClass(LongWritable.class);
                JOB_max_frnd_2.setMapOutputValueClass(Text.class);

                JOB_max_frnd_2.setOutputKeyClass(Text.class);
                JOB_max_frnd_2.setOutputValueClass(LongWritable.class);

                JOB_max_frnd_2.setInputFormatClass(KeyValueTextInputFormat.class);
                // Sorting it in the Decreasing Order of the Common Friends Count
                JOB_max_frnd_2.setSortComparatorClass(LongWritable.DecreasingComparator.class);

                JOB_max_frnd_2.setNumReduceTasks(1);

                // setting the HDFS path for the Result Text Data
                FileInputFormat.addInputPath(JOB_max_frnd_2, new Path(Arguments[1]));
                FileOutputFormat.setOutputPath(JOB_max_frnd_2, new Path(Arguments[2]));

                //Waiting till the completion of te Job
                System.exit(JOB_max_frnd_2.waitForCompletion(true) ? 0 : 1);
            }
        }
    }
}

//    How to Run The Code:
//    hadoop jar Maximum_Friends_Pair.jar Maximum_Friends_Pair /shivam/input/soc-LiveJournal1Adj.txt /shivam/interques2 /shivam/output2
