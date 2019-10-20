package core;


import config.KafkaProperties;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @Author: wds
 * @Description:
 * @Date: created in 18:27 2019/10/8
 */
public class KafkaConsumerContainer<T> extends Thread {

    private KafkaProperties kafkaProperties;
    private KafkaConsumer consumer;
    private ThreadPoolExecutor executor;
    private String topic;
    private String groupId;
    private String clientId;
    private MessageHandler messageHandler;
    private volatile boolean polled;
    private boolean isAutoCommit;

    private KafkaConsumerContainer(){}

    public KafkaConsumerContainer(KafkaProperties kafkaProperties, ThreadPoolExecutor executor, String topic, String groupId, String clientId, MessageHandler messageHandler) {
        this.kafkaProperties = kafkaProperties;
        this.executor = executor;
        this.topic = topic;
        this.groupId = groupId;
        this.clientId = clientId;
        this.messageHandler = messageHandler;
        this.polled = true;
        init();
    }

    private void init() {
        Properties properties = this.kafkaProperties.buildConsumerProperties();
        properties.put("group.id", this.groupId);
        properties.put("client.id", this.clientId);
        this.isAutoCommit = (boolean) properties.get("enable.auto.commit");
        this.consumer = new KafkaConsumer(properties);
        this.consumer.subscribe(Arrays.asList(this.topic));

    }

    private void pollAndInvoke() {
        while (polled) {
            ConsumerRecords<String, T> records = this.consumer.poll(Duration.ofSeconds(1));
            System.out.println(records.count());
            MessagedoHandle messagedoHandle = new MessagedoHandle(messageHandler, records);
            executor.submit(messagedoHandle);
            if (!isAutoCommit) {
                this.consumer.commitSync();
            }
        }
    }

    @Override
    public void run() {
        pollAndInvoke();
        System.out.println("this container is closed");
        this.consumer.close();
        this.executor.shutdown();
        System.out.println("bye bye");
    }

    public void close() {
        this.polled = false;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((groupId == null) ? 0 : groupId.hashCode());
        result = prime * result + ((topic == null) ? 0 : topic.hashCode());
        result = prime * result + ((clientId == null) ? 0 : clientId.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        KafkaConsumerContainer other = (KafkaConsumerContainer) obj;
        if (groupId == null) {
            if (other.groupId != null) {
                return false;
            }
        } else if (!groupId.equals(other.groupId)) {
            return false;
        }
        if (topic == null) {
            if (other.topic != null) {
                return false;
            }
        } else if (!topic.equals(other.topic)) {
            return false;
        }
        if (clientId == null) {
            if (other.clientId != null) {
                return false;
            }
        } else if (!clientId.equals(other.clientId)) {
            return false;
        }
        return true;
    }
}
