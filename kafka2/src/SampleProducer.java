import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Producer Example in Apache Kafka
 * 
 * @author www.tutorialkart.com
 */
public class SampleProducer extends Thread {
	private final KafkaProducer<Integer, String> producer;
	private final String topic;
	private final Boolean isAsync;

	public static final String KAFKA_SERVER_URL = "pc011.fyre.ibm.com";
	public static final int KAFKA_SERVER_PORT = 9092;
	public static final String CLIENT_ID = "SampleProducer";

	public SampleProducer(String topic, Boolean isAsync) {
		Properties properties = new Properties();
		properties.put("bootstrap.servers", KAFKA_SERVER_URL + ":" + KAFKA_SERVER_PORT);
		properties.put("client.id", CLIENT_ID);
		properties.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//		properties.put("retries",0);
//		properties.put("request.timeout.ms", 5000);
		producer = new KafkaProducer<Integer, String>(properties);
		this.topic = topic;
		this.isAsync = isAsync;
	}

	public void run() {
		int messageNo = 1;
		int i = 20;
		while (i > 0) {
			i--;
			String messageStr = "Aparna" + messageNo;
			// long startTime = System.currentTimeMillis();
			if (isAsync) { // Send asynchronously

				
					producer.send(new ProducerRecord<Integer, String>(topic, messageStr),
							new DemoCallBack(0, messageNo, messageStr));
				
				try {
					TimeUnit.SECONDS.sleep(10);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

				System.out.println("AsynchronousProducer call completed");

			} else { // Send synchronously
				try {
					RecordMetadata metadata = (RecordMetadata) producer
							.send(new ProducerRecord<Integer, String>(topic,messageNo, messageStr)).get();

					try {
						TimeUnit.SECONDS.sleep(5);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					System.out.println("Sent message: (" + messageNo + ", " + messageStr + ")");

					System.out.println("Message is sent to Partition no " + metadata.partition() + " and offset "
							+ metadata.offset());
					System.out.println("SynchronousProducer Completed with success.");

				} catch (InterruptedException | ExecutionException e) {
					e.printStackTrace();
					System.out.print("Caught exception in synchronous producer");
					// handle the exception
				}
			}
			++messageNo;
		}
	}
}

class DemoCallBack implements Callback {

	private final long startTime;
	private final int key;
	private final String message;

	public DemoCallBack(long startTime, int key, String message) {
		this.startTime = startTime;
		this.key = key;
		this.message = message;
	}

	/**
	 * onCompletion method will be called when the record sent to the Kafka Server
	 * has been acknowledged.
	 *
	 * @param metadata  The metadata contains the partition and offset of the
	 *                  record. Null if an error occurred.
	 * @param exception The exception thrown during processing of this record. Null
	 *                  if no error occurred.
	 */
	public void onCompletion(RecordMetadata metadata, Exception exception) {
		long elapsedTime = System.currentTimeMillis() - startTime;
		if (metadata != null) {

			System.out.println("Callback::::message(" + key + ", " + message + ") sent to partition("
					+ metadata.partition() + "), " + "offset(" + metadata.offset() + ") in " + elapsedTime + " ms");
		} else {
			exception.printStackTrace();
			System.out.println("Exception in asynchronous producer caught");
		}
	}
}