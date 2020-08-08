import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import java.time.Duration;
import java.util.*;

public class CommitAsyncExample {
	private static String TOPIC_NAME = "test2";
	private static KafkaConsumer<String, String> consumer;
	private static TopicPartition topicPartition;

	public static void main(String[] args) throws Exception {

		System.out.print("I am consumer target");

		String groupName = "RG";
		Properties props = new Properties();
		props.put("bootstrap.servers", "host:9092");
		props.put("group.id", groupName);
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("enable.auto.commit", "false");

		consumer = new KafkaConsumer<>(props);

		topicPartition = new TopicPartition(TOPIC_NAME, 0);
		consumer.assign(Collections.singleton(topicPartition));
		printOffsets("before consumer loop", consumer, topicPartition);
		// sendMessages();
	//	startConsumer();
	}

	private static void startConsumer() {
		try {
			while (true) {
				@SuppressWarnings("deprecation")
				ConsumerRecords<String, String> records = consumer.poll(100);
				for (ConsumerRecord<String, String> record : records) {
					System.out.println("Topic:" + record.topic() + " Partition:" + record.partition() + " Offset:"
							+ record.offset() + " Value:" + record.value());
				}
				printOffsets("before commitAsync() call", consumer, topicPartition);
				consumer.commitAsync();
				printOffsets("after commitAsync() call", consumer, topicPartition);
			}

		} catch (Exception e) {

		}
		// printOffsets("after consumer loop", consumer, topicPartition);
	}

	private static void printOffsets(String message, KafkaConsumer<String, String> consumer,
			TopicPartition topicPartition) {
		
		Map<TopicPartition, OffsetAndMetadata> committed = consumer
				.committed(new HashSet<>(Arrays.asList(topicPartition)));
		OffsetAndMetadata offsetAndMetadata = committed.get(topicPartition);
		long position = consumer.position(topicPartition);
		System.out.printf("Offset info %s, Committed: %s, current position %s%n", message,
				offsetAndMetadata == null ? null : offsetAndMetadata.offset(), position);
	}

//  private static void sendMessages() {
//      Properties producerProps = ExampleConfig.getProducerProps();
//      KafkaProducer producer = new KafkaProducer<>(producerProps);
//      for (int i = 0; i < 4; i++) {
//          String value = "message-" + i;
//          System.out.printf("Sending message topic: %s, value: %s%n", TOPIC_NAME, value);
//          producer.send(new ProducerRecord<>(TOPIC_NAME, value));
//      }
//      producer.flush();
//      producer.close();
//  }
}
