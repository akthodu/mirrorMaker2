import java.io.InputStream;
import java.time.Duration;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.mirror.MirrorClient;
import org.apache.kafka.connect.mirror.RemoteClusterUtils;

public class TranslateOffset {

	public static void main(String[] args) throws Exception{
    		System.out.print("I am checkpoint consumer at target : source.checkpoints.internal");
            String topicName = "test2";
            KafkaConsumer<String, String> consumer = null;
            
            String groupName = "RG";
            Properties props = new Properties();
            props.put("bootstrap.servers", "host21:9092");
            props.put("group.id", groupName);
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

            consumer = new KafkaConsumer<>(props);
            
            consumer.subscribe(Arrays.asList(topicName));
            
            Map<Object, Object> prop = new Properties();
            InputStream input;
        //    HashMap<String, Object> propvals = new HashMap<String, Object>();
            
            input = TranslateOffset.class.getResourceAsStream("mm2.properties");
            ((Properties) prop).load(input);
            System.out.println("Property File Loaded Succesfully");
            
            Map<String, Object> propertiesMap = new HashMap<>();
            
            for (Entry<Object, Object> entry : prop.entrySet()) {
            	propertiesMap.put((String) entry.getKey(),  entry.getValue());
            }
            
            
//            Map<String, Object> propertiesMap = new HashMap<>();
//     //       propertiesMap = prop;
//            
            
            
            String consumerGroupId=groupName;
			String oldClusterName = "source";
//			Map<String, Object> newClusterProperties = new HashMap<>();
//			newClusterProperties.put("bootstrap.servers", "pc011.fyre.ibm.com:9092");
//			//newClusterProperties.put("consumer.client.id", "mm2-client");
			Duration duration = Duration.ofHours(1);
			
		//	Set<String> checkPointTopics = RemoteClusterUtils.checkpointTopics(propertiesMap);
			int hops= RemoteClusterUtils.replicationHops(propertiesMap, "source");
			
			int i=5;
		while(i>0) {
			Map<TopicPartition, OffsetAndMetadata> newOffsets = RemoteClusterUtils.translateOffsets(
            		   propertiesMap, oldClusterName, consumerGroupId, duration
            		);
		
			System.out.print(newOffsets);
			i--;
	//		TopicPartition topicPartition = new TopicPartition("source.test2", 0);
			//consumer.seek(newOffsets, 0);
     //       		consumer.seek(topicPartition,0);
		}
            
	}    


}
