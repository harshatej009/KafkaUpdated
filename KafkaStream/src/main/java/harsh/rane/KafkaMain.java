package harsh.rane;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;

public class KafkaMain {

	public static void main(String[] args) throws InterruptedException {
		
		Properties streamsConfiguration = new Properties();
		streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "KafkaMain");
		streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		
		
		
		StreamsBuilder builder = new StreamsBuilder();  // building Kafka Streams Model
		KStream<String, String> left = builder.stream("topic2");
	    KStream<String, String> right = builder.stream("topic3");	
	    
	    KStream<String, String> wordCounts = left.flatMapValues(value -> Arrays.asList(value.toLowerCase()));
	    wordCounts.to("topicresult");
        
        KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration); //Starting kafka stream
        streams.start();
        
        
        /*./bin/kafka-console-consumer --bootstrap-server localhost:9092
        --topic topicresult
        --from-beginning
        --formatter kafka.tools.DefaultMessageFormatter
        --property print.key=true
        --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer 
        --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer*/
      //  .\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties

        //.\bin\windows\kafka-server-start.bat .\config\server.properties
	}

}
