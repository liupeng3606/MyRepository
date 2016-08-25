package com.frontsurf;

import com.frontsurf.util.Conf;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

/**
 * Kafka消息生产者
 * Created by Liupeng on 2016/8/9.
 */
public class KafkaProducer {
    private final Producer<Integer, String> producer;
    /**
     * 全局配置
     */
    private Conf conf = Conf.getInstance();

    /**
     * 构造方法中初始化 producer
     */
    public KafkaProducer(){
        Properties props = new Properties();
        //配置kafka broker
        props.put("metadata.broker.list", conf.getKafkaBrokeList());
        //序列化类
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        producer = new Producer<Integer, String>(new ProducerConfig(props));
    }

    /**
     * 根据指定消息数目，产生消息往kafka中发送
     * @param msgNum
     */
    public void produce(int msgNum){
        int count = 1;
        while (count<=msgNum){
            String msg = "This is a test message,number is \t"+ (count++);
            producer.send(new KeyedMessage<Integer, String>(conf.getTopic(), msg));
            try{
                //延迟发送，模拟真实场景
                Thread.sleep(conf.getSleep_time());
            }catch (Throwable e){
                //do nothing
            }
            System.out.println("Send message------"+msg);
        }
    }

    public static void main(String[] args) {
        System.out.println("------Producer start produce message------");
        KafkaProducer kafkaProducer = new KafkaProducer();
        kafkaProducer.produce(kafkaProducer.conf.getMessageNum());
    }
}
