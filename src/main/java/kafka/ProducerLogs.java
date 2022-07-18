package kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.Future;

public class ProducerLogs {
    private final Producer<String, String> producer;
    private final String topic;
    private final Boolean isAsync;

    public static final String KAFKA_SERVER = "m1:9092,m2:9092";
    public static final String CLIENT_ID = "SampleProducer";

    /**
     * Contructor
     * @param topic
     * @param isAsync
     */
    public ProducerLogs(String topic, Boolean isAsync) {
        Properties props = new Properties();

        props.put("bootstrap.servers", KAFKA_SERVER);
        props.put("auto.create.topics.enable", false);
        props.put("client.id", CLIENT_ID);
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<String, String>(props);
        this.topic = topic;
        this.isAsync = isAsync;
    }

    /**
     * Đọc dữ liệu từ folder và ghi vào Kafka Broker
     * @throws IOException
     */
    public void run() throws IOException {
        String target_dir = "sample-text";
        File dir = new File(target_dir);
        File[] files = dir.listFiles();

        for (File f : files) {
            if(f.isFile()) {
                BufferedReader inputStream = null;

                try {
                    inputStream = new BufferedReader(
                            new FileReader(f));
                    String line;

                    while ((line = inputStream.readLine()) != null) {
//                        System.out.println(line);
                        if (isAsync) {
                            producer.send(new ProducerRecord<>(topic, line), new ProdcucerCallback());
                        }
                        else {
                            Future<RecordMetadata> future = producer.send(new ProducerRecord<>(topic, line));

                            // We perform a get() on the future object, which turns the send call synchronous
                            RecordMetadata recordMetadata = future.get();

                            // The RecordMetadata object contains the offset and partition for the message.
                            System.out.println(String.format("Message written to partition %s with offset %s", recordMetadata.partition(),
                                    recordMetadata.offset()));
                        }

//                        Thread.sleep(1000);
                    }
                }
                catch (Exception e) {
                    System.out.println("Exception sending message " + e.getMessage());
                }
                finally {
                    if (inputStream != null) {
                        inputStream.close();
                    }
                    producer.flush();
//                    producer.close();
                }
            }
        }
    }

    public static void main(String[] args) throws IOException {
        ProducerLogs producerLogs = new ProducerLogs("sample-data", true);
        producerLogs.run();
    }
}
