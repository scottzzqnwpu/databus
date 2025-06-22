package com.my.indexer;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class CanalToKafkaIndexer {
    private static final Logger logger = LoggerFactory.getLogger(CanalToKafkaIndexer.class);

    // 配置参数
    private static Properties config = new Properties();
    private static volatile boolean running = true;
    private static CanalConnector connector;
    private static KafkaProducer<String, String> producer;

    public static void main(String[] args) {
        // 加载配置文件
        if (!loadConfig()) {
            System.exit(1);
        }

        // 添加关闭钩子
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            running = false;
            closeResources();
        }));

        int retryCount = 0;
        int maxRetries = Integer.parseInt(config.getProperty("max.retries", "5"));

        while (running && retryCount <= maxRetries) {
            try {
                initialize();
                processMessages();
            } catch (Exception e) {
                logger.error("Main process error, retry count: " + retryCount, e);
                closeResources();

                if (++retryCount <= maxRetries) {
                    try {
                        long interval = Long.parseLong(config.getProperty("retry.interval.ms", "5000"));
                        TimeUnit.MILLISECONDS.sleep(interval);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                } else {
                    logger.error("Max retries reached, exiting...");
                }
            }
        }
    }

    private static boolean loadConfig() {
        String externalPath = System.getProperty("config.file");
        InputStream input = null;
        if (externalPath != null) {
            logger.info("loading config.properties from externalPath:" + externalPath);
            try {
                input = Files.newInputStream(Paths.get(externalPath));
            } catch (IOException e) {
                logger.error("Failed to load external config file: " + externalPath, e);
                return false;
            }
        } else {
            logger.info("loading config.properties from classpath");
            input = CanalToKafkaIndexer.class.getClassLoader().getResourceAsStream("config.properties");
        }
        if (input == null) {
            logger.error("Sorry, unable to find config.properties");
            return false;
        }

        // 加载配置文件
        try {
            config.load(input);
        } catch (IOException e) {
            logger.error("Failed to load configuration from input stream", e);
            return false;
        }

        // 验证必要配置
        if (!validateConfig()) {
            return false;
        }
        logger.info("Configuration loaded successfully");
        return true;
    }

    private static boolean validateConfig() {
        String[] requiredProps = {
                "canal.server.host", "canal.server.port", "canal.destination",
                "kafka.bootstrap.servers", "kafka.topic"
        };

        for (String prop : requiredProps) {
            if (!config.containsKey(prop)) {
                logger.error("Missing required configuration: " + prop);
                return false;
            }
        }

        return true;
    }

    private static void initialize() {
        // 初始化 Canal 连接
        String canalHost = config.getProperty("canal.server.host");
        int canalPort = Integer.parseInt(config.getProperty("canal.server.port"));
        String destination = config.getProperty("canal.destination");

        connector = CanalConnectors.newSingleConnector(
                new InetSocketAddress(canalHost, canalPort),
                destination, "", "");

        // 初始化 Kafka producer
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", config.getProperty("kafka.bootstrap.servers"));
        kafkaProps.put("acks", config.getProperty("kafka.acks", "all"));
        kafkaProps.put("retries", config.getProperty("kafka.retries", "3"));
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // Timeout configurations
        kafkaProps.put("request.timeout.ms", config.getProperty("kafka.request.timeout.ms", "30000"));  // default 30 seconds
        kafkaProps.put("delivery.timeout.ms", config.getProperty("kafka.delivery.timeout.ms", "120000"));  // default 2 minutes
        kafkaProps.put("metadata.fetch.timeout.ms", config.getProperty("kafka.metadata.fetch.timeout.ms", "60000"));  // default 1 minute

        logger.info("Kafka properties loaded successfully: " + kafkaProps);
        producer = new KafkaProducer<>(kafkaProps);

        // 连接 Canal
        connectWithRetry();
    }

    private static void connectWithRetry() {
        int maxRetries = Integer.parseInt(config.getProperty("max.retries", "5"));
        int retry = 0;

        while (running && retry < maxRetries) {
            try {
                connector.connect();
                String filter = config.getProperty("canal.subscribe.filter", ".*\\..*");
                connector.subscribe(filter);
                connector.rollback();
                logger.info("Connected to Canal server successfully");
                return;
            } catch (Exception e) {
                retry++;
                logger.error("Failed to connect to Canal server, retry " + retry, e);
                if (retry < maxRetries) {
                    try {
                        long interval = Long.parseLong(config.getProperty("retry.interval.ms", "5000"));
                        TimeUnit.MILLISECONDS.sleep(interval);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }

        if (retry >= maxRetries) {
            throw new RuntimeException("Failed to connect to Canal server after " + maxRetries + " retries");
        }
    }

    private static void processMessages() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        int batchSize = Integer.parseInt(config.getProperty("canal.batch.size", "100"));
        long timeout = Long.parseLong(config.getProperty("canal.timeout.ms", "3000"));
        long sleepTimeNoData = Long.parseLong(config.getProperty("sleep.time.no.data.ms", "1000"));
        String kafkaTopic = config.getProperty("kafka.topic");

        while (running) {
            try {
                Message message = connector.getWithoutAck(batchSize);
                long batchId = message.getId();
                int size = message.getEntries().size();

                if (batchId == -1 || size == 0) {
                    TimeUnit.MILLISECONDS.sleep(sleepTimeNoData);
                    continue;
                }

                try {
                    processMessageBatch(message, mapper, kafkaTopic);
                    connector.ack(batchId);
                } catch (Exception e) {
                    logger.error("Process message error, rollback batch: " + batchId, e);
                    connector.rollback(batchId);
                }
            } catch (Exception e) {
                logger.error("Get message error, will try to reconnect", e);
                closeResources();
                initialize();
            }
        }
    }

    private static void processMessageBatch(Message message, ObjectMapper mapper, String kafkaTopic) throws Exception {
        for (CanalEntry.Entry entry : message.getEntries()) {
            if (entry.getEntryType() != CanalEntry.EntryType.ROWDATA) {
                continue;
            }

            CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            CanalEntry.EventType eventType = rowChange.getEventType();
            String tableName = entry.getHeader().getTableName();

            for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                Map<String, String> data = new HashMap<>();
                List<CanalEntry.Column> columns = eventType == CanalEntry.EventType.DELETE
                        ? rowData.getBeforeColumnsList()
                        : rowData.getAfterColumnsList();

                for (CanalEntry.Column column : columns) {
                    data.put(column.getName(), column.getValue());
                }

                Map<String, Object> indexMsg = new HashMap<>();
                indexMsg.put("op", eventType.toString().toLowerCase());
                indexMsg.put("table", tableName);
                indexMsg.put("id", data.get("id"));
                indexMsg.put("fields", data);
                indexMsg.put("type", eventType);

                String json = mapper.writeValueAsString(indexMsg);
                ProducerRecord<String, String> record = new ProducerRecord<>(kafkaTopic, data.get("id"), json);

                try {
                    producer.send(record, (metadata, exception) -> {
                        if (exception != null) {
                            logger.error("Failed to send message to Kafka: " + json, exception);
                        } else {
                            logger.debug("Successfully sent message to Kafka: " + json);
                        }
                    });
                    logger.info("Processed event: " + indexMsg);
                } catch (Exception e) {
                    logger.error("Failed to send message to Kafka", e);
                    throw e;
                }
            }
        }
    }

    private static void closeResources() {
        try {
            if (connector != null) {
                connector.disconnect();
                connector = null;
            }
        } catch (Exception e) {
            logger.error("Error while closing Canal connector", e);
        }
        try {
            if (producer != null) {
                producer.close();
                producer = null;
            }
        } catch (Exception e) {
            logger.error("Error while closing Kafka producer", e);
        }
    }
}