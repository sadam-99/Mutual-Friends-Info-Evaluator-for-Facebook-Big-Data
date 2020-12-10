// Code Author:-
// Name: Shivam Gupta
// Net ID: SXG190040
// CS 6350.001 - Big Data Management and Analytics - F20 Assignment 1 (Hadoop MapReduce)


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class InMemoryJoin extends Configured implements Tool {

    //Job1
    static String Friend1 = "";
    static String Friend2 = "";
    public static class Job1_MapClass extends Mapper<LongWritable, Text, Text, Text> {
        private Text USER_Frnd_ID = new Text();
        private Text Friends_List = new Text();

        public void map(LongWritable key, Text value, Context Data) throws IOException, InterruptedException {
            Configuration CONFIG = Data.getConfiguration();
            Friend1=CONFIG.get("USER1_ID");
            Friend2 = CONFIG.get("USER2_ID");
            String[] USER = value.toString().split("\t");
            if ((USER.length == 2)&&(USER[0].equals(Friend1)||USER[0].equals(Friend2))) {

                String[] friend_list = USER[1].split(",");
                Friends_List.set(USER[1]);
                for (int i = 0; i < friend_list.length; i++) {
                    String Hash_Map_Key;
                    if (Integer.parseInt(USER[0]) < Integer.parseInt(friend_list[i])) {
                        Hash_Map_Key = USER[0] + "," + friend_list[i];
                    } else {
                        Hash_Map_Key = friend_list[i] + "," + USER[0];
                    }
                    USER_Frnd_ID.set(Hash_Map_Key);
                    Data.write(USER_Frnd_ID, Friends_List);
                }


            }
        }
    }

    public static class Job1_Reducer extends Reducer<Text, Text, Text, Text> {
        private Text OUTPUT = new Text();

        public void reduce(Text key, Iterable<Text> values, Context Data) throws IOException, InterruptedException {
            HashSet HASH_MAP = new HashSet();
            int i = 0;

            String Result ="";
            for (Text value : values) {
                String[] val = value.toString().split(",");
                for (int j = 0; j < val.length; j++) {
                    if (i == 0) {
                        HASH_MAP.add(val[j]);
                    } else {
                        if (HASH_MAP.contains(val[j])) {
                            Result=Result.concat(val[j]);
                            Result=Result.concat(",");
                            HASH_MAP.remove(val[j]);
                        }

                    }
                }
                i++;
            }
            if(!Result.equals("")){
                OUTPUT.set(Result);
                Data.write(key, OUTPUT);
            }
        }

    }



    static HashMap<String, String> USER_INFO;

    //Job2
    public static class Job2_MapClass extends Mapper<LongWritable, Text, Text, Text> {
        private Text Friends_List = new Text();
        private Text USER_Frnd_ID = new Text();

        public void map(LongWritable key, Text value, Context Data) throws IOException, InterruptedException {
            Configuration CONFIG = Data.getConfiguration();
            USER_INFO = new HashMap<String, String>();
            String userDataPath =CONFIG.get("userdata");
            FileSystem file_sys = FileSystem.get(CONFIG);
            // Path path = new Path("hdfs://localhost:9000"+userDataPath);
            Path path = new Path(CONFIG.get("userdata"));
            BufferedReader br = new BufferedReader(new InputStreamReader(file_sys.open(path)));
            String Friends_Line = br.readLine();
            while (Friends_Line != null) {
                String[] Friend_Array = Friends_Line.split(",");
                if (Friend_Array.length == 10) {
                    String data = Friend_Array[1] + ":" + Friend_Array[9];
                    USER_INFO.put(Friend_Array[0].trim(), data);
                }
                Friends_Line = br.readLine();
            }

            String[] USER = value.toString().split("\t");
            if ((USER.length == 2)) {

                String[] friend_list = USER[1].split(",");
                int L = friend_list.length;
                StringBuilder Result=new StringBuilder("[");
                for (int i = 0; i < friend_list.length; i++) {
                    if(USER_INFO.containsKey(friend_list[i]))
                    {
                        if(i==(friend_list.length-1))
                            Result.append(USER_INFO.get(friend_list[i]));
                        else
                        {
                            Result.append(USER_INFO.get(friend_list[i]));
                            Result.append(",");
                        }
                    }
                }
                Result.append("]");
                USER_Frnd_ID.set(USER[0]);
                Friends_List.set(Result.toString());
                Data.write(USER_Frnd_ID, Friends_List);
            }

        }
    }

    // Main Function for the Driver MApReduce Code for in-Memory Join for List of Common Friends
    public static void main(String[] args) throws Exception {
        int out = ToolRunner.run(new Configuration(), new InMemoryJoin(), args);
        System.exit(out);

    }

    @Override
    public int run(String[] Arguments) throws Exception {
        Configuration Config1 = new Configuration();
        // String[] Arguments = new GenericOptionsParser(Config1, args).getRemainingArgs();
        // Taking all the required Arguments
        if (Arguments.length != 6) {
            System.err.println("Please give the Valit Inputs:  <USER1_ID> <USER2_ID> <soc_data_file> <Intermediateoutput> <userdata> <Finaloutput>");
            System.exit(2);
        }
        Config1.set("USER1_ID", Arguments[0]);
        Config1.set("USER2_ID", Arguments[1]);
        
        // Creating a Config_Job_1 with name "InMemoryJoin"
        Job Config_Job_1 = new Job(Config1, "InMemoryJoin");
        Config_Job_1.setJarByClass(InMemoryJoin.class);
        Config_Job_1.setMapperClass(Job1_MapClass.class);
        Config_Job_1.setReducerClass(Job1_Reducer.class);

        // setting the type of output keys
        Config_Job_1.setOutputKeyClass(Text.class);
        // setting the type of output Values
        Config_Job_1.setOutputValueClass(Text.class);
        // setting the HDFS path for input data
        FileInputFormat.addInputPath(Config_Job_1, new Path(Arguments[2]));
        // setting the HDFS path for the Result Text Data
        Path p=new Path(Arguments[3]);
        FileOutputFormat.setOutputPath(Config_Job_1, p);
        //Waiting till the completion of the Job
        int CODE = Config_Job_1.waitForCompletion(true) ? 0 : 1;


        // Creating a Config_Job_2 with name "InMemoryJoin"

        Configuration Config2 = getConf();
        Config2.set("userdata", Arguments[4]);
        Job Config_Job_2 = new Job(Config2, "InMemoryJoin");
        Config_Job_2.setJarByClass(InMemoryJoin.class);

        // setting the type of output Keys and Values
        Config_Job_2.setMapperClass(Job2_MapClass.class);
        //Config_Job_2.setReducerClass(Reduce.class);
        Config_Job_2.setOutputKeyClass(Text.class);
        Config_Job_2.setOutputValueClass(Text.class);

        // setting the HDFS path for the input Data and Result Text Data
        FileInputFormat.addInputPath(Config_Job_2,p);
        FileOutputFormat.setOutputPath(Config_Job_2, new Path(Arguments[5]));

        //Waiting till the completion of the Job
        CODE = Config_Job_2.waitForCompletion(true) ? 0 : 1;
        System.exit(CODE);
        return CODE;

    }
}


//    How to Run Code:
//    hadoop jar InMemoryJoin.jar InMemoryJoin 0 38 /shivam/input/soc-LiveJournal1Adj.txt /shivam/intermed_inmem /shivam/input/userdata.txt /shivam/output3
