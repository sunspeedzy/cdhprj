package com.cloudera.zy.kafka_krb

import kafka.utils.ZkUtils
/**
  * 从Zookeeper里读取Kafka Topic的分区信息
  * 使用 kafka.utils.ZkUtils 读取Topic信息
  */
object GetOffsetFromZK {
  def main(args: Array[String]): Unit = {
    // 例如 master-01:2181
    val zkQuorum = args(0)
    val topicNameArr = args(1).split(",")

    val zkUrl = zkQuorum
    // zkUrl：以逗号分隔的Zookeeper Server信息，如 127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002。
    //        端口号后可以加chroot命令要指定的根目录，如 127.0.0.1:3001,127.0.0.1:3002/app/a，
    //        这样客户端的操作都会在 /app/a下进行。
    // sessionTimeout    是客户端和zookeeper连接后会话的timeout，默认为30000毫秒
    // connectionTimeout 是客户端连接zookeeper时的timeout，默认为Integer.MAX_VALUE毫秒
    val zkClientAndConnection = ZkUtils.createZkClientAndConnection(zkUrl, 30000, 5000)
    val zkUtils = new ZkUtils(zkClientAndConnection._1, zkClientAndConnection._2, false)
    val zKNumberOfPartitionsForTopic = zkUtils.getPartitionsForTopics(topicNameArr)

    zkUtils.getPartitionAssignmentForTopics(topicNameArr).foreach { topicAndPartitionMap =>
      val topic = topicAndPartitionMap._1
      val partitionMap = topicAndPartitionMap._2
      println("partition assignment of /brokers/topics/%s is %s".format(topic, partitionMap))
    }

    zkClientAndConnection._1.close()
    zkClientAndConnection._2.close()

    topicNameArr.foreach(topicName=>{
      println(zKNumberOfPartitionsForTopic.get(topicName))
    })

  }
}
