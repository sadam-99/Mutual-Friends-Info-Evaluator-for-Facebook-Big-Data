# <!-- // Code Author:-
# // Name: Shivam Gupta
# // Net ID: SXG190040
# // CS 6350.001 - Big Data Management and Analytics - F20 Assignment 1 (Hadoop MapReduce) -->

## Implementation of Hadoop MapReduce in JAVA

JAVA Version USED: ```JAVA Version "1.8" ```


## Putting the Input Files into Hadoop Run Commands:
```hdfs dfs -mkdir /shivam/input```
```hdfs dfs -put soc-LiveJournal1Adj.txt /shivam/input```
```hdfs dfs -put userdata.txt /shivam/input```
        
## How to Use and Run the Scripts:
 
 ### For Question 1(Mutual Friends) Run Commands:
 hadoop jar <jarname> <Class_Name> <soc_datatxt_file_path> <Final_output_path>
 for eg: ```hadoop jar MutualFriends.jar MutualFriends /shivam/input/soc-LiveJournal1Adj.txt /shivam/output1```

 ### For Question 2(Friends Pairs with Maximum Common Friends) Run Commands:
 hadoop jar <jarname> <Class_Name> <soc_datatxt_file_path> <Intermediateoutput> <Final_output_path>
 for eg:```hadoop jar Maximum_Friends_Pair.jar Maximum_Friends_Pair /shivam/input/soc-LiveJournal1Adj.txt /shivam/interques2 /shivam/output2```


  ### For Question 3(In Memory join at the Mapper for Information about Mutual Freinds) Run Commands:
 hadoop jar <jarname> <Class_Name> <USER1_ID> <USER2_ID> <soc_datatxt_file_path> <Intermediateoutput> <userdata_filepath> <Final_output__path>
 for eg:```hadoop jar InMemoryJoin.jar InMemoryJoin 0 38 /shivam/input/soc-LiveJournal1Adj.txt /shivam/intermed_inmem /shivam/input/userdata.txt /shivam/output3```


  ### For Question 4(Friends with Maximum Age of Direct Friends-In Memory join at the Reducer) Run Commands:
 hadoop jar <jarname> <Class_Name> <soc_datatxt_file_path> <userdata_filepath> <Final_output__path>
 for eg:```hadoop jar MaximumAge_Direct_Friends.jar MaximumAge_Direct_Friends /shivam/input/soc-LiveJournal1Adj.txt /shivam/input/userdata.txt /shivam/output4```


## Getting the Output Files from Hadoop to local machine Run Commands:
```hdfs dfs -get /shivam/output```



# Note: 

There are 4 Folders containing their respective 4 Solutions:
```Question1``` , ```Question2```, ```Question3```, ```Question4```
## Results (Output Files:)
 Each folder has a java and a jar file. Each folder also has a folder named ```output``` inside which there is a File named ```part-r-00000``` which contains output of that Question

