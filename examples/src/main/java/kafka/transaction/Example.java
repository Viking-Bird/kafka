package kafka.transaction;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;

/**
 * @author pengwang
 * @date 2020/11/01
 */
public class Example {

    public static void main(String[] args) {
//        Producer<String, String> producer = new KafkaProducer<String, String>(props);
//
//        // 初始化事务，包括结束该Transaction ID对应的未完成的事务（如果有）
//        // 保证新的事务在一个正确的状态下启动
//        producer.initTransactions();
//
//        // 开始事务
//        producer.beginTransaction();
//
//        // 消费数据
//        ConsumerRecords<String, String> records = consumer.poll(100);
//
//        try {
//            // 发送数据
//            producer.send(new ProducerRecord<String, String>("Topic", "Key", "Value"));
//
//            // 发送消费数据的Offset，将上述数据消费与数据发送纳入同一个Transaction内
//            producer.sendOffsetsToTransaction(offsets, "group1");
//
//            // 数据发送及Offset发送均成功的情况下，提交事务
//            producer.commitTransaction();
//        } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
//            // 数据发送或者Offset发送出现异常时，终止事务
//            producer.abortTransaction();
//        } finally {
//            // 关闭Producer和Consumer
//            producer.close();
//            consumer.close();
//        }
    }
}