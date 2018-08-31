package com.ccq.apache.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Date;
import java.util.Properties;

public class KafkaCustomerTest {
    public static void main(String[] args) {
        Properties props = new Properties();

        props.put("bootstrap.servers", "192.168.220.140:9092");
        System.out.println("this is the group part test 1");
        //消费者的组id
        props.put("group.id", "GroupB");//这里是GroupA或者GroupB
        
        //设置不自动提交，自己手动更新offset
        props.put("enable.auto.commit", "false");
        //props.put("enable.auto.commit", "true");
        
        props.put("auto.commit.interval.ms", "1000");
        //从poll(拉)的回话处理时长
        props.put("session.timeout.ms", "30000");
        //poll的数量限制
        props.put("max.poll.records", "5");

        props.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");

        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        
        /**
         * earliest 当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费 
         * &&根据offset的值,没有就从头消费,有就从offset位置开始消费
         * latest 当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据 
         * &&只消费新数据
         * none topic各分区都存在已提交的offset时，从offset后开始消费；只要有一个分区不存在已提交的offset，则抛出异常
         * &&全部分区都有offset才能消费
         * */
        props.setProperty("auto.offset.reset", "earliest"); 
        
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        //订阅主题列表topic
        consumer.subscribe(Arrays.asList("test"));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                //　正常这里应该使用线程池处理，不应该在这里处理
            	// topic有多个partition分区,会导致排序出错.因为数据被分配在不同的分区
                System.out.printf("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value() + "\n");
                
            }//手动提交offset
            consumer.commitSync();
        }
        
        
    }
}
