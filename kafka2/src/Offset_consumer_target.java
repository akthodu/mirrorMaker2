import java.util.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.mirror.RemoteClusterUtils;

public class Offset_consumer_target{
    
    
    public static void main(String[] args) throws Exception{
    		System.out.print("I am offset consumer");
            //String topicName = "source.checkpoints.internal";
            String topicName = "mm2-offsets.source.internal";
            KafkaConsumer<String, String> consumer = null;
            
            String groupName = "RG";
            Properties props = new Properties();
            props.put("bootstrap.servers", "host21:9092");
            props.put("group.id", groupName);
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("enable.auto.commit", "false");

            consumer = new KafkaConsumer<>(props);
         //   RebalanceListner rebalanceListner = new RebalanceListner(consumer);
            
            consumer.subscribe(Arrays.asList(topicName));
            try{
                while (true){
                	
              //  	Map<String, Object> newClusterProperties = null;
//					String oldClusterName= "source";
//					String consumerGroupId = "RG";
//					Map<TopicPartition, OffsetAndMetadata> newOffsets = RemoteClusterUtils.translateOffsets(
//                			   newClusterProperties, oldClusterName, consumerGroupId, null
//                			);
                	
                			//consumer.seekToBeginning(null);
                	
                	//System.out.print("I am consumer");
                    @SuppressWarnings("deprecation")
					ConsumerRecords<String, String> records = consumer.poll(100);
                    for (ConsumerRecord<String, String> record : records){
                        System.out.println("Topic:"+ record.topic() +" Partition:" + record.partition() + " Offset:" + record.offset() + " Value:"+ record.value());
                       // Do some processing and save it to Database
                      //  rebalanceListner.addOffset(record.topic(), record.partition(),record.offset());
                    }
                    
                        //consumer.commitSync(rebalanceListner.getCurrentOffsets());
                }
            }catch(Exception ex){
                System.out.println("Exception.");
                ex.printStackTrace();
            }
            finally{
                    consumer.close();
            }
    }
    
}
