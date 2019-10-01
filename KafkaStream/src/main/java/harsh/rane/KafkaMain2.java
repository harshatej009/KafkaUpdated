package harsh.rane;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class KafkaMain2
{
	
	private static final Logger LOGGER = LogManager.getLogger(KafkaMain2.class);
	
	public static void main(String[] args)
	{
		Duration timeDifferenceMs = null ;
		Properties streamsConfiguration = new Properties();
		streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "KafkaMain");
		streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
	
		StreamsBuilder builder = new StreamsBuilder();  // building Kafka Streams Model
		
		KStream<Long, String> stream1 = builder.stream("topic2");   //get Stream
	    KStream<Long, String> stream2 = builder.stream("topic3");	 //get Stream
	    
	    KStream<Long, Object> joined = stream1.join(stream2,(leftValue, rightValue) -> "left=" + leftValue + ", right=" + rightValue, JoinWindows.of(timeDifferenceMs.ofMinutes(1)));
	    	 
	    joined.to("topicresult");
	   
        
        KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration); //Starting kafka stream
        streams.start();
	}

}
