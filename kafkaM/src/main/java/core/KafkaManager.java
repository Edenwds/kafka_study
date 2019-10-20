package core;

import config.KafkaProperties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.StringUtils;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Author: wds
 * @Description:
 * @Date: created in 17:14 2019/10/8
 */
public class KafkaManager implements IKafkaManager {

    private static final Logger logger = LoggerFactory.getLogger(KafkaManager.class);

    private KafkaProperties properties;
    private KafkaProducer producer;
    private Map<String, Map<String, ThreadPoolExecutor>> topicConsumerMap = new HashMap<>();  // Map<topic, Map<groupId, pool>>
    private Map<String, Map<String, KafkaConsumerContainer>> groupContainerMap = new HashMap<>(); // Map<groupId, Map<clientId, container>>
    private Map<String, AtomicInteger> counterMap = new HashMap<>();

    private KafkaManager(){}

    public KafkaManager(KafkaProperties properties) {
        this.properties = properties;
        initProducer(properties);
    }

    private void initProducer(KafkaProperties properties) {
        Properties ps = properties.buildProducerProperties();
        this.producer = new KafkaProducer(ps);
    }

    @Override
    public int sendMessage(String topic, String message) {
        return sendMessage(topic, Arrays.asList(message));
    }

    @Override
    public int sendMessage(String topic, List<String> messages) {
        return sendMessage(topic, null, messages);
    }

    @Override
    public int sendMessage(String topic, String key, String message) {
        Map<String, String> map = new HashMap<>();
        map.put(key, message);
        return sendMessage(topic, map);
    }

    @Override
    public int sendMessage(String topic, Map<String, String> messages) {
        return sendMessage(topic, null, messages);
    }

    @Override
    public int sendMessage(String topic, Integer partition, List<String> messages) {
        try {
            for (String message : messages) {
                ProducerRecord record = new ProducerRecord(topic, partition, null, message);
                this.producer.send(record);
            }
        } catch (Exception e) {
            logger.error("topic {}, partition {} messages send error ! ", topic, partition, e);
            return -1;
        }
        return 1;
    }

    @Override
    public int sendMessage(String topic, Integer partition, Map<String, String> messages) {
        try {
            for (Map.Entry message : messages.entrySet()) {
                ProducerRecord record = new ProducerRecord(topic, partition, message.getKey(), message.getValue());
                this.producer.send(record);
            }
        } catch (Exception e) {
            logger.error("topic {}, partition {} messages send error ! ", topic, partition, e);
            return -1;
        }
        return 1;
    }

    @Override
    public <T> int sendMessageObject(String topic, T message) {
        return 0;
    }

    @Override
    public <T> int sendMessageObject(String topic, List<T> messages) {
        return 0;
    }

    @Override
    public <T> int sendMessageObject(String topic, Integer partition, List<T> messages) {
        return 0;
    }

    @Override
    public <T> int sendMessageObject(String topic, String key, T message) {
        return 0;
    }

    @Override
    public <T> int sendMessageObject(String topic, Integer partition, Map<String, T> messages) {
        return 0;
    }

    @Override
    public <T> int subscribe(MessageHandler<T> messageHandler, String groupId, String topic) {
        return subscribe(messageHandler, groupId, topic, null);
    }

    @Override
    public <T> int subscribe(MessageHandler<T> messageHandler, String groupId, String topic, String clientId) {
        return subscribe(messageHandler, groupId, topic, clientId, 1);
    }

    @Override
    public <T> int subscribe(MessageHandler<T> messageHandler, String groupId, String topic, String clientId, Integer concurrency) {
        return subscribe(messageHandler, groupId, topic, clientId, 1, null);
    }

    @Override
    public <T> int subscribe(MessageHandler<T> messageHandler, String groupId, String topic, String clientId, Integer concurrency, Integer partitionCount) {
        if (partitionCount == null || partitionCount <= 0) {
            partitionCount = 3;
        }
        if (concurrency == null || concurrency <= 0) {
            concurrency = 1;
        }
        Map<String, ThreadPoolExecutor> topicConsumerPool = topicConsumerMap.get(topic);
        if (topicConsumerPool == null) {
            topicConsumerPool = new ConcurrentHashMap<>();
            topicConsumerMap.put(topic, topicConsumerPool);
        }
        ThreadPoolExecutor consumerPool = topicConsumerPool.get(groupId);
        if (consumerPool == null) {
            consumerPool = new ThreadPoolExecutor(partitionCount, partitionCount, 0L, TimeUnit.MILLISECONDS, new SynchronousQueue<>(), new ThreadPoolExecutor.AbortPolicy());
            topicConsumerPool.put(groupId, consumerPool);
        }
        Map<String, KafkaConsumerContainer> groupContainer = groupContainerMap.get(groupId);
        if (groupContainer == null) {
            groupContainer = new ConcurrentHashMap<>();
            groupContainerMap.put(groupId, groupContainer);
        }

        if (StringUtils.isEmpty(clientId)) {
            AtomicInteger counter = getCounter(groupId, topic);
            StringJoiner joiner = new StringJoiner("-", "", "-").add(groupId).add(topic).add("consumer");
            clientId = joiner.toString() + counter.getAndIncrement();
        }
        if (groupContainer.get(clientId) != null) {
            throw new RuntimeException("The clientId has been used");
        }
        ThreadPoolExecutor handlePool = new ThreadPoolExecutor(concurrency, concurrency, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(100), new ThreadPoolExecutor.CallerRunsPolicy());
        KafkaConsumerContainer consumerContainer = new KafkaConsumerContainer(this.properties, handlePool, topic, groupId, clientId, messageHandler);
        groupContainer.put(clientId, consumerContainer);
        consumerPool.submit(consumerContainer);
        return 1;
    }

    private AtomicInteger getCounter(String groupId, String topic) {
        String key = groupId + "-" + topic;
        AtomicInteger counter = counterMap.get(key);
        if (counter == null) {
            counter = new AtomicInteger(1);
            counterMap.put(key, counter);
        }
        return counter;
    }

    @Override
    public int unSubscribe(String topic) {
        return unSubscribe(topic, null);
    }

    @Override
    public int unSubscribe(String topic, String groupId) {
        return unSubscribe(topic, groupId, null);
    }

    @Override
    public int unSubscribe(String topic, String groupId, String clientId) {
        try {
            if (clientId != null) {
                if (groupId == null) {
                    throw new RuntimeException("ClientId has value but groupId is null when unsubscribe");
                }
                Map<String, KafkaConsumerContainer> groupContainer = groupContainerMap.get(groupId);
                KafkaConsumerContainer consumerContainer = groupContainer.get(clientId);
                if (consumerContainer == null) {
                    throw new RuntimeException("The clientId's container is no exist");
                }
                consumerContainer.close();
            } else if (groupId != null) {
                if (StringUtils.isEmpty(topic)) {
                    throw new RuntimeException("Topic is null when unsubscribe only by groupId");
                }
                Map<String, ThreadPoolExecutor> topicConsumerPool = topicConsumerMap.get(topic);
                ThreadPoolExecutor consumerPool = topicConsumerPool.get(groupId);
                Map<String, KafkaConsumerContainer> containerMap = groupContainerMap.get(groupId);
                for (KafkaConsumerContainer container : containerMap.values()) {
                    container.close();
                }
                consumerPool.shutdown();
            } else {
                Map<String, ThreadPoolExecutor> topicConsumerPool = topicConsumerMap.get(topic);
                for (Map.Entry<String, ThreadPoolExecutor> topicConsumer : topicConsumerPool.entrySet()) {
                    String gid = topicConsumer.getKey();
                    Map<String, KafkaConsumerContainer> containerMap = groupContainerMap.get(gid);
                    for (KafkaConsumerContainer container : containerMap.values()) {
                        container.close();
                    }
                    ThreadPoolExecutor executor = topicConsumer.getValue();
                    executor.shutdown();
                }

            }
        } catch (Exception e) {
            return 0;
        }

        return 1;
    }
}
