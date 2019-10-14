package com.cloudera.zy.kafka_krb

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}


object MyProducer {
  def main(args: Array[String]): Unit = {
    // 例如 node-01:9092
    val bootStrapServers = args(0)
    val topicName = args(1)
    val eventCnt = args(2).toInt

    System.setProperty("java.security.krb5.conf", "C:\\Program Files\\MIT\\Kerberos\\krb5.ini")
    System.setProperty("java.security.auth.login.config", "jaas-cache.conf")
    System.setProperty("javax.security.auth.useSubjectCredsOnly", "false")
    System.setProperty("sun.security.krb5.debug", "true")

    val props = new Properties
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers)
    props.put(ProducerConfig.ACKS_CONFIG, "all")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put("security.protocol", "SASL_PLAINTEXT")
    props.put("sasl.kerberos.service.name", "kafka")

    val producer = new KafkaProducer[String, String](props)
    for (i <- (0 until eventCnt).indices) {
      val key = "key-" + i
      val value = "Value-" + i
      val record = new ProducerRecord[String, String](topicName, key, value)
      producer.send(record)
      System.out.println(key + "----" + value)

      if (i % 10 ==0)
        producer.flush()
    }

    producer.close()
  }
}
