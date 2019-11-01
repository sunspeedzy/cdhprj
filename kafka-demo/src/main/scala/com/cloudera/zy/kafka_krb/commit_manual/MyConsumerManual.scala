package com.cloudera.zy.kafka_krb.commit_manual

import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition

/**
  * 从启用了Kerberos的Kafka中消费数据
  * 由Consumer手动提交Offset
  */
object MyConsumerManual {
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
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "DemoConsumerGrp-CommitManual")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringDeserializer")
    // 启动自动提交，会周期性地在后台提交consumer's offset //
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    /////////////////////////////////////////////////////////////////
    props.put("security.protocol", "SASL_PLAINTEXT")
    props.put("sasl.kerberos.service.name", "kafka")

    val consumer = new KafkaConsumer[String, String](props)
    // 只从 Topic 订阅消息，自动负载Partition
    consumer.subscribe(util.Arrays.asList(topicName))

    import scala.collection.JavaConversions._
    try {
      while (true) {
        try {
          Thread.sleep(800)
          println()
          val records = consumer.poll(Long.MaxValue)

          records.partitions.foreach(recp => {
            val partitionRecords = records.records(recp)
            partitionRecords.foreach(rec => {
              println("Received message: (" + rec.key() + ", " + rec.value() + ") at offset " + rec.offset()
                + " of patition " + rec.partition() + ". Topic is " + rec.topic())
            })
            /* 同步确认某个分区的特定的offset */
            val lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset() + 1
            consumer.commitSync(Map[TopicPartition, OffsetAndMetadata](recp -> new OffsetAndMetadata(lastOffset)))
            println("Commit offset：Topic-" + recp.topic() + ", Partition-" + recp.partition() + ", Offset-" + lastOffset)
          })
        } catch {
          case ex: Exception =>
            ex.printStackTrace()
        }
      }
    } finally {
      consumer.close()
    }
  }
}
