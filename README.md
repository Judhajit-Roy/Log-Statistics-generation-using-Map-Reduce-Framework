# CS441_HW1

This repo has 4 different Tasks using Map-Reduce Framework.

### Instructions:

Instructions for installing the and implementing the Tasks.
- Clone the project using command: git clone git@github.com:Judhajit-Roy/CS441_HW1.git
- Open corresponging shell for Mac/linux/windows (I used windows powershell) and navigate to the root directory.
- In the root directory run command: "sbt assembly"
- This generates a JAR file at target/scala-3.0.0/"jarfile"
- To run the 4 tasks you can use the object names MapReduceProgram1,MapReduceProgram2,MapReduceProgram3,MapReduceProgram4
- For Task 1, it takes 2 parameters: input files path and output folder path
- For Task 2, it takes 3 parameters: input files path, intermediate path and output folder path
- For Task 3, it takes 2 parameters: input files path and output folder path
- For Task 4, it takes 2 parameters: input files path and output folder path

### Running Tasks:

For task1 : "hadoop jar "jarfilename".jar MapReduceProgram1 /input /output
where /input is folder containing log files, /output is folder name where output is stored

For task2 : "hadoop jar "jarfilename".jar MapReduceProgram2 /input /intermediateoutput /output
where /input is folder containing log files, /output is folder name where output is stored

For task3 : "hadoop jar "jarfilename".jar MapReduceProgram3 /input /output
where /input is folder containing log files, /output is folder name where output is stored

For task4 : "hadoop jar "jarfilename".jar MapReduceProgram4 /input /output
where /input is folder containing log files, /output is folder name where output is stored

Here is video explaining the code and running jar on AWS https://youtu.be/grw2Vo2m1Jo

### Description of Tasks:

#### Task1

Log files are accepted as input. The log message type of logs that fit the established pattern are mapped during the mapper phase. The message's timestamp is grabbed and compared to the fixed starttime and endtime obtained from the config. If the particular line satifies all conditions it is counted as 1 and passed to the reducer as (key,value) pair ie (INFO or DEBUG or ERROR or WARN,1). The reducer groups together all pairs of same key type and sums them and final output is (messagetype, sum)

For example: 
DEBUG,13
ERROR,10

#### Task1

Log files are accepted as input. The logs logs of type are only considered"ERROR" is grabbed alongmessage's timestamp is grabbed and compared to the fixed starttime and endtime obtained from the config. If the particular line satifies all conditions it is counted as 1 and passed to the reducer as (key,value) pair ie (INFO or DEBUG or ERROR or WARN,1). The reducer groups together all pairs of same key type and sums them and final output is (messagetype, sum)

For example: 
DEBUG,13
ERROR,10

Task 3

Accepts input from log files. The log message type is mapped to number one in the mapper phase. This is aggregated in the reduction step to get the total number of each type of log message. Output: DEBUG,59093 \sINFO,413448 \sWARN,68786 \sERROR,49549

Task 4

Accepts input from log files. The message string that matches the predetermined pattern is mapped to the message type in the mapper phase. For each type, the reducer determines the length of the longest string and outputs that information as the result.

Output includes DEBUG,18 INFO,15 WARN,15, and ERROR,24.

Implementing AWS

Use "sbt assembly" to create the jar file, and then use this as input for the AWS EMR step as seen in the movie. The inputs may be transferred into the cluster's master node or stored on S3. While the application is running, provide the appropriate input routes.



