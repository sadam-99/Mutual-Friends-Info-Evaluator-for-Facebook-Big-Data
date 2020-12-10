// Code Author:-
// Name: Shivam Gupta
// Net ID: SXG190040
// CS 6350.001 - Big Data Management and Analytics - F20 Assignment 1 (Hadoop MapReduce)


import java.util.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Array;
import java.net.URI;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MutualFriends {

    public static class Map
            extends Mapper<LongWritable, Text, Text, Text>{

        private Text Friends_ID_Pair = new Text();
        private Text Frnd_list = new Text();

        private boolean Is_ID_Exists(String key) {
            String[] ID_Pairs = {"0,1","20,28193", "1,29826", "6222,19272", "28041,28056"};

            for (String pair : ID_Pairs) {
                if (key.equals(pair)) {
                    return true;
                }
            }
            return false;
        }

        public void map(LongWritable key, Text value, Context Data) throws IOException, InterruptedException {
            String[] Text_File_Line = value.toString().split("\t");
 
            if(Text_File_Line.length == 2)
            {
                String user = Text_File_Line[0];
                List<String> Friends_IDs_List =  Arrays.asList(Text_File_Line[1].split(","));
                for(String Friend_ID : Friends_IDs_List)
                {
                    String pair;
                    int friend1_ID = Integer.parseInt(user);
                    int friend2_ID = Integer.parseInt(Friend_ID);

                    if (friend1_ID < friend2_ID)
                    {
                        Friends_ID_Pair.set(friend1_ID + "," + friend2_ID);
                        pair = String.valueOf(friend1_ID) + "," + String.valueOf(friend2_ID);
                    }
                    else
                    {
                        Friends_ID_Pair.set(friend2_ID + "," + friend1_ID);
                        pair = String.valueOf(friend2_ID) + "," + String.valueOf(friend1_ID);
                    }
                    if (Is_ID_Exists(pair))
                    {
                    Data.write(Friends_ID_Pair,new Text(Text_File_Line[1]));
                    }
                }
            }
        }
    }

    public static class Reduce
            extends Reducer<Text,Text,Text,Text> {

        private Text Output = new Text();

        public void reduce(Text key, Iterable<Text> values, Context Data) throws IOException, InterruptedException {

            HashMap<String,Integer> Hash_Map_friends = new HashMap<String,Integer>();
            StringBuilder text_out = new StringBuilder();
            for(Text frnds : values)
            {
                List<String> friends_IDs_List = Arrays.asList(frnds.toString().split(","));
                for(String friend_ID : friends_IDs_List)
                {
                    if (Hash_Map_friends.containsKey(friend_ID))
                    {
                        text_out.append(friend_ID + ",");
                    }
                    else
                    {
                        Hash_Map_friends.put(friend_ID,1);
                    }
                }
            }
            if (text_out.lastIndexOf(",") > -1)
            {
                text_out.deleteCharAt(text_out.lastIndexOf(","));
            }
            Output.set( new Text(text_out.toString()));
            Data.write(key, Output);
        }
    }

    // Main Function for the Driver MApReduce Code
    public static void main(String[] args) throws Exception {
        Configuration Config_Job = new Configuration();
        String[] Arguments = new GenericOptionsParser(Config_Job, args).getRemainingArgs();
        // Taking all the required Arguments
        if (Arguments.length != 2) {
            System.err.println("Please Give the Valid Format of Inputs:  <soc_data_input_File> <output_path>");
            System.exit(2);
        }

        // Creating a job with name "MutualFriends"
        Job Job_MF = new Job(Config_Job, "MutualFriends");
        Job_MF.setJarByClass(MutualFriends.class);
        Job_MF.setMapperClass(MutualFriends.Map.class);
        Job_MF.setReducerClass(MutualFriends.Reduce.class);



        // setting the type of output key 
        Job_MF.setOutputKeyClass(Text.class);
        // setting the type of output value
        Job_MF.setOutputValueClass(Text.class);
        //setting the HDFS path of the input Text Data
        FileInputFormat.addInputPath(Job_MF, new Path(Arguments[0]));
        // setting the HDFS path for the Result Text Data
        FileOutputFormat.setOutputPath(Job_MF, new Path(Arguments[1]));
        //Waiting till the completion of te Job
        System.exit(Job_MF.waitForCompletion(true) ? 0 : 1);
    }
}


//    How to Run the code:
//    hadoop jar MutualFriends.jar MutualFriends /shivam/input/soc-LiveJournal1Adj.txt /shivam/output1
