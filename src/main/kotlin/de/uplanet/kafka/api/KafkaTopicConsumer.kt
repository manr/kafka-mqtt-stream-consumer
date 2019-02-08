package de.uplanet.kafka.api

import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import redis.clients.jedis.JedisPool
import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.*


class KafkaTopicConsumer(p_props: Properties,
                         p_topics: List<ConsumerTopic>,
                         p_jedisPool: JedisPool) : Runnable {

    private val consumer = KafkaConsumer<String, GenericRecord>(p_props)
    private val topics = p_topics
    private val jedisPool = p_jedisPool
    private val limitsByTopic = topics.associate { ct -> (ct.name to ct.limit) }

    override fun run() {
        consumer.subscribe(topics.map { ct -> ct.name })

        val jedisClient = jedisPool.resource
        jedisClient.use { jedis ->
            consumer.use { consumer ->
                while (true) {
                    val records = consumer.poll(Duration.of(1, ChronoUnit.MINUTES))

                    if (records != null) {
                        val topicList = mutableSetOf<String>()
                        for (record in records) {
                            println(record.value())

                            jedis.lpush(record.topic(), record.value().toString())
                            val limit = limitsByTopic[record.topic()] ?: 0
                            jedis.ltrim(record.topic(), 0, limit)
                            topicList.add(record.topic())
                        }

                        jedis.publish("TOPIC_UPDATES", topicList.joinToString())
                    }
                }
            }
        }
    }
}