package de.uplanet.kafka.api

import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import redis.clients.jedis.JedisPool
import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.*


class KafkaTopicConsumer(p_props: Properties,
                         p_topic: String,
                         p_limit: Long,
                         p_jedisPool: JedisPool) : Runnable {

    private val consumer = KafkaConsumer<String, GenericRecord>(p_props)
    private val topic = p_topic
    private val jedisPool = p_jedisPool
    private val limit = p_limit

    override fun run() {
        consumer.subscribe(listOf(topic))

        val jedisClient = jedisPool.resource
        jedisClient.use { jedis ->
            consumer.use { consumer ->
                while (true) {
                    val records = consumer.poll(Duration.of(1, ChronoUnit.MINUTES))

                    if (records != null) {
                        for (record in records) {
                            println(record.value())

                            jedis.lpush(topic, record.value().toString())
                            jedis.ltrim(topic, 0, limit)
                        }
                    }
                }
            }
        }
    }
}