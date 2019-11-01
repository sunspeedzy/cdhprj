package com.cloudera.zy.kafka_krb.commit_auto

import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.TopicPartition

/**
  * 从启用了Kerberos的Kafka中消费数据
  * 由Consumer自动提交Offset
  */
object MyConsumerAuto {
  def main(args: Array[String]): Unit = {
    // 例如 node-01:9092
    val bootStrapServers = args(0)
    val topicName = args(1)

    // 配置Kerberos的conf（ini）文件
    System.setProperty("java.security.krb5.conf", "C:\\Program Files\\MIT\\Kerberos\\krb5.ini")
    // jaas的配置文件，指明JAAS所需要的登录模块
    System.setProperty("java.security.auth.login.config",
      "D:\\gitRepo\\cdhprj\\kafka-demo\\out\\production\\resources\\jaas.conf")
    System.setProperty("javax.security.auth.useSubjectCredsOnly", "false")
    // 调试Kerberos，true则打印调试信息
    System.setProperty("sun.security.krb5.debug", "false")

    val props = new Properties
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "DemoConsumerGrp-CommitAuto")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringDeserializer")
    // 启动自动提交，会周期性地在后台提交consumer's offset //
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    // 设置自动提交的周期为 1000ms //
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
    /////////////////////////////////////////////////////////////////
    props.put("security.protocol", "SASL_PLAINTEXT")
    props.put("sasl.kerberos.service.name", "kafka")

    val consumer = new KafkaConsumer[String, String](props)

    val partition0 = new TopicPartition(topicName, 0)
    val partition1 = new TopicPartition(topicName, 1)
    val partition2 = new TopicPartition(topicName, 2)

    // 向特定的分区订阅消息
    consumer.assign(util.Arrays.asList(partition0, partition1, partition2))

    while(true) {
      try {
        Thread.sleep(200)
        println()
        val records = consumer.poll(Long.MaxValue)
        val recIter = records.iterator()
        while (recIter.hasNext){
          val rec = recIter.next()
          println("Received message: (" + rec.key() + ", " + rec.value() + ") at offset " + rec.offset()
            + " of patition " + rec.partition())
        }
      } catch {
        case ex: Exception =>
          ex.printStackTrace()
      }
    }
  }
}
