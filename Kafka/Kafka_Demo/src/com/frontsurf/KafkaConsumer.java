package com.frontsurf;

import com.frontsurf.util.Conf;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Kafka消息消费者
 * Created by Liupeng on 2016/8/9.
 */
public class KafkaConsumer {
    private final ConsumerConnector consumer;
    /**
     * 全局配置
     */
    private Conf conf = Conf.getInstance();

    /**
     * 构造方法中初始化 consumer
     */
    public KafkaConsumer(){
        Properties props = new Properties();
        //配置zookeeper
        props.put("zookeeper.connect", conf.getZkHosts());
        //配置消费组
        props.put("group.id", conf.getGroupId());
        //序列化类
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("zookeeper.session.timeout.ms", "4000");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");

        ConsumerConfig config = new ConsumerConfig(props);
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(config);
    }

    /**
     * 从kafka中消费消息
     */
    public void consume(){
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(conf.getTopic(), new Integer(1));

        Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = consumer.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream = messageStreams.get(conf.getTopic()).get(0);
        ConsumerIterator<byte[], byte[]> iterator = stream.iterator();
        while (iterator.hasNext()){
            MessageAndMetadata<byte[], byte[]> data = iterator.next();
            System.out.println("Get message------"+new String(data.message()));
        }
    }

    public static void main(String[] args) {
        System.out.println("------Consumer start consume message------");
        new KafkaConsumer().consume();
    }
}
