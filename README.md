mvn clean && mvn assembly:assembly -DdescriptorId=jar-with-dependencies

java -jar target/kafka-consumer-1.0-SNAPSHOT-jar-with-dependencies.jar
