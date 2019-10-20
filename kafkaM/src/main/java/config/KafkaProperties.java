package config;


import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import util.StringUtils;

import java.util.Properties;

/**
 * @Author: wds
 * @Description:
 * @Date: created in 11:20 2019/10/14
 */
public class KafkaProperties {

    private static String bootstrapServers;
    private final KafkaProperties.Producer producer = new KafkaProperties.Producer();
    private final KafkaProperties.Consumer consumer = new KafkaProperties.Consumer();

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public Producer getProducer() {
        return producer;
    }

    public Consumer getConsumer() {
        return consumer;
    }

    public Properties buildProducerProperties() {
        return this.producer.buildProperties();
    }

    public Properties buildConsumerProperties() {
        return this.consumer.buildProperties();
    }

    public class Consumer {
        private String bootstrapServers;
        private String autoCommitInterval;
        private String enableAutoCommit;
        private String keyDeserializer;
        private String valueDeserializer;

        public String getBootstrapServers() {
            return bootstrapServers;
        }

        public void setBootstrapServers(String bootstrapServers) {
            this.bootstrapServers = bootstrapServers;
        }

        public String getAutoCommitInterval() {
            return autoCommitInterval;
        }

        public void setAutoCommitInterval(String autoCommitInterval) {
            this.autoCommitInterval = autoCommitInterval;
        }

        public String getEnableAutoCommit() {
            return enableAutoCommit;
        }

        public void setEnableAutoCommit(String enableAutoCommit) {
            this.enableAutoCommit = enableAutoCommit;
        }

        public String getKeyDeserializer() {
            return keyDeserializer;
        }

        public void setKeyDeserializer(String keyDeserializer) {
            this.keyDeserializer = keyDeserializer;
        }

        public String getValueDeserializer() {
            return valueDeserializer;
        }

        public void setValueDeserializer(String valueDeserializer) {
            this.valueDeserializer = valueDeserializer;
        }

        public Properties buildProperties() {
            Properties ps = new Properties();
            String bootstrapServers = StringUtils.isEmpty(this.getBootstrapServers()) ? KafkaProperties.this.getBootstrapServers() : this.getBootstrapServers();
            if (StringUtils.isEmpty(bootstrapServers)) {
                throw new RuntimeException("bootstrap.servers value is null");
            }
            ps.put("bootstrap.servers", bootstrapServers);
            String enableAutoCommit = StringUtils.isEmpty(this.getEnableAutoCommit()) ? "true" : this.getEnableAutoCommit();
            ps.put("enable.auto.commit", Boolean.valueOf(enableAutoCommit));
            String autoCommitInterval = this.getAutoCommitInterval();
            if (!StringUtils.isEmpty(autoCommitInterval)) {
                if (!StringUtils.isInteger(autoCommitInterval)) {
                    throw new RuntimeException("auto.commit.interval value must be Integer");
                }
                ps.put("auto.commit.interval.ms", Integer.parseInt(autoCommitInterval));
            }
            String keyDeserializer = this.getKeyDeserializer();
            if (StringUtils.isEmpty(keyDeserializer)) {
                keyDeserializer = StringDeserializer.class.getName();
            }
            ps.put("key.deserializer", keyDeserializer);
            String valueDeserializer = this.getValueDeserializer();
            if (StringUtils.isEmpty(valueDeserializer)) {
                valueDeserializer = StringDeserializer.class.getName();
            }
            ps.put("value.deserializer", valueDeserializer);
            return ps;
        }
    }



    private class Producer {
        private String bootstrapServers;
        private String acks;
        private String batchSize;
        private String bufferMemory;
        private String keySerializer;
        private String valueSerializer;
        private String retries;
        private String clientId;

        public Producer() {
        }

        public String getBootstrapServers() {
            return bootstrapServers;
        }

        public void setBootstrapServers(String bootstrapServers) {
            this.bootstrapServers = bootstrapServers;
        }

        public String getAcks() {
            return acks;
        }

        public void setAcks(String acks) {
            this.acks = acks;
        }

        public String getBatchSize() {
            return batchSize;
        }

        public void setBatchSize(String batchSize) {
            this.batchSize = batchSize;
        }

        public String getBufferMemory() {
            return bufferMemory;
        }

        public void setBufferMemory(String bufferMemory) {
            this.bufferMemory = bufferMemory;
        }

        public String getKeySerializer() {
            return keySerializer;
        }

        public void setKeySerializer(String keySerializer) {
            this.keySerializer = keySerializer;
        }

        public String getValueSerializer() {
            return valueSerializer;
        }

        public void setValueSerializer(String valueSerializer) {
            this.valueSerializer = valueSerializer;
        }

        public String getRetries() {
            return retries;
        }

        public void setRetries(String retries) {
            this.retries = retries;
        }

        public String getClientId() {
            return clientId;
        }

        public void setClientId(String clientId) {
            this.clientId = clientId;
        }

        public Properties buildProperties() {
            Properties ps = new Properties();
            String bootstrapServers = StringUtils.isEmpty(this.getBootstrapServers()) ? KafkaProperties.this.getBootstrapServers() : this.getBootstrapServers();
            if (StringUtils.isEmpty(bootstrapServers)) {
                throw new RuntimeException("bootstrap.servers value is null");
            }
            ps.put("bootstrap.servers", bootstrapServers);
            String acks = this.getAcks();
            if (!StringUtils.isEmpty(acks)) {
                ps.put("acks", acks);
            }
            String retries = this.getRetries();
            if (!StringUtils.isEmpty(retries)) {
                if (!StringUtils.isInteger(retries)) {
                    throw new RuntimeException("retries value must be Integer");
                }
                ps.put("retries", Integer.parseInt(retries));
            }
            String batchSize = this.getBatchSize();
            if (!StringUtils.isEmpty(batchSize)) {
                if (!StringUtils.isInteger(batchSize)) {
                    throw new RuntimeException("batch.size value must be Integer");
                }
                ps.put("batch.size", Integer.parseInt(batchSize));
            }
            String clientId = this.getClientId();
            if (!StringUtils.isEmpty(clientId)) {
                ps.put("client.id", clientId);
            }
            String keySerializer = this.getKeySerializer();
            if (StringUtils.isEmpty(keySerializer)) {
                keySerializer = StringSerializer.class.getName();
            }
            ps.put("key.serializer", keySerializer);
            String valueSerializer = this.getValueSerializer();
            if (StringUtils.isEmpty(valueSerializer)) {
                valueSerializer = StringSerializer.class.getName();
            }
            ps.put("value.serializer", valueSerializer);

            return ps;
        }
    }

}
