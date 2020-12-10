// Code Author:-
// Name: Shivam Gupta
// Net ID: SXG190040
// CS 6350.001 - Big Data Management and Analytics - F20 Assignment 1 (Hadoop MapReduce)


import java.io.BufferedReader;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.io.InputStreamReader;
import java.io.IOException;
import java.text.ParseException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class MaximumAge_Direct_Friends {

    // Job1 for finding the Maximum age of direct friends

    public static class Job1_MapClass extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text values, Context Data) throws IOException, InterruptedException {
            String[] Friends_ID = values.toString().split("\t");
            if (Friends_ID.length == 2) {
                Data.write(new Text(Friends_ID[0]), new Text(Friends_ID[1]));
            }
        }
    }

    public static class Job1_Reducer extends Reducer<Text, Text, Text, Text> {

        private int AGE_Generation(String tex) throws ParseException {

            Calendar today = Calendar.getInstance();
            SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");
            Date Parsed_DATE = sdf.parse(tex);
            Calendar DOB = Calendar.getInstance();
            DOB.setTime(Parsed_DATE);

            int YEAR_Current = today.get(Calendar.YEAR);
            int DOB_YEAR = DOB.get(Calendar.YEAR);
            int Calc_AGE = YEAR_Current - DOB_YEAR;

            int MONTH_CURRENT = today.get(Calendar.MONTH);
            int DOB_MONTH = DOB.get(Calendar.MONTH);
            if (DOB_MONTH > MONTH_CURRENT) { // this year will not be counted
                Calc_AGE--;
            } else if (DOB_MONTH == MONTH_CURRENT) { // if it is the same month then check for the day
                int DAY_CURRENT = today.get(Calendar.DAY_OF_MONTH);
                int DOB_DAY = DOB.get(Calendar.DAY_OF_MONTH);
                if (DOB_DAY > DAY_CURRENT) { // this year will not be counted!
                    Calc_AGE--;
                }
            }
            return Calc_AGE;
        }

        static HashMap<Integer, Integer> HASH_MAP_AGE = new HashMap<Integer, Integer>();

        protected void setup(Context Data) throws IOException, InterruptedException {
            super.setup(Data);
            Configuration CONF = Data.getConfiguration();
            // Location of the file in HDFS System 
            Path Location = new Path(CONF.get("Data"));
            FileSystem fs = FileSystem.get(CONF);
            FileStatus[] File_Sys = fs.listStatus(Location);
            for (FileStatus status : File_Sys) {
                Path PT = status.getPath();
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(PT)));
                String USER_DATA_Line;
                USER_DATA_Line = br.readLine();
                while (USER_DATA_Line != null) {
                    String[] info = USER_DATA_Line.split(",");
                    if (info.length == 10) {
                        try {
                            int Calc_AGE = AGE_Generation(info[9]);
                            HASH_MAP_AGE.put(Integer.parseInt(info[0]), Calc_AGE);
                        } catch (ParseException Excep) {
                            Excep.printStackTrace();
                        }
                    }
                    USER_DATA_Line = br.readLine();
                }
            }
        }

        public void reduce(Text key, Iterable<Text> values, Context Data) throws IOException, InterruptedException {
            for (Text Direct_Friends : values) {
                String[] Friends_IDs_List = Direct_Friends.toString().split(",");
                int maximum_age = 0;
                int Calc_AGE;
                for (String Friend_ID : Friends_IDs_List) {
                    Calc_AGE = HASH_MAP_AGE.get(Integer.parseInt(Friend_ID));

                    if(Calc_AGE > maximum_age)
                    {
                        maximum_age = Calc_AGE;
                    }
                }
                Data.write(key, new Text(Integer.toString(maximum_age)));
            }
        }
    }


    // Main Function for the Driver MApReduce Code for Maximum age of the Direct Friends
    public static void main(String[] args) throws Exception {
        Configuration Config = new Configuration();
        String[] Arguments = new GenericOptionsParser(Config, args).getRemainingArgs();
        if (Arguments.length != 3) {
            System.err.println("Please give the Valid Format of Inputs: <Soc_user_data File> <userdata.txt> <Final_output_Path>");
            System.exit(2);
        }
        Config.set("Data", Arguments[1]);
        // Creating a Config_Job with name "MaximumAgeInMemory"
        Job Config_Job = Job.getInstance(Config, "MaximumAgeInMemory");

        // setting the type of output keys and Values
        
        Config_Job.setJarByClass(MaximumAge_Direct_Friends.class);
        Config_Job.setMapperClass(Job1_MapClass.class);
        Config_Job.setReducerClass(Job1_Reducer.class);
        // setting the HDFS path for input data and output text Data
        FileInputFormat.addInputPath(Config_Job, new Path(Arguments[0]));
        FileOutputFormat.setOutputPath(Config_Job, new Path(Arguments[2]));

        // setting the type of output keys and Values
        Config_Job.setMapOutputKeyClass(Text.class);
        Config_Job.setMapOutputValueClass(Text.class);

        Config_Job.setOutputKeyClass(Text.class);
        Config_Job.setOutputValueClass(Text.class);
        //Waiting till the completion of te Job
        if (!Config_Job.waitForCompletion(true)) {
            System.exit(1);
        }
    }
}

// How to Run the Code:
// hadoop jar MaximumAge_Direct_Friends.jar MaximumAge_Direct_Friends /shivam/input/soc-LiveJournal1Adj.txt /shivam/input/userdata.txt /shivam/output4
