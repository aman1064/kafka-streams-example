# kafka-streams-example
This project demonstrates the Window Aggregation of Kafka Streams. The window used is a TimeWindow (other windows are Sliding and Session). This is a fixed time window of one minute.

## Running this Project
> - You should have **JAVA 8** and **Maven** installed on your system to build this project. 
> - To Build run **mvn clean package** and the final artifact will be present in target directory of the project with name **event-windowing-0.0.1-SNAPSHOT-jar-with-dependencies.jar**
> - Beofre running this project enure that you have a topic with name **raw_clicks** with atleast 8 partitions 
> - To run type **java -jar event-windowing-0.0.1-SNAPSHOT-jar-with-dependencies.jar /path/to/streaming.properties /path/to/topics.properties totalNumberOfInstances**
> - A dummy producer is present, to generate raw click stream. 
