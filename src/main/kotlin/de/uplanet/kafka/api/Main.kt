package de.uplanet.kafka.api

import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.javalin.Context
import io.javalin.Javalin
import org.apache.kafka.common.serialization.StringDeserializer
import redis.clients.jedis.JedisPool
import redis.clients.jedis.JedisPoolConfig
import java.util.*
import java.util.concurrent.Executors


object Main {
    @JvmStatic
    fun main(args: Array<String>) {
        val jedisPool = JedisPool(JedisPoolConfig(), "localhost")

        val kafkaProps = Properties()
        kafkaProps["bootstrap.servers"] = "localhost:29092"
        kafkaProps["group.id"] = "consumer-tutorial"
        kafkaProps["auto.offset.reset"] = "latest"
        kafkaProps["key.deserializer"] = StringDeserializer::class.java.name
        kafkaProps["value.deserializer"] = KafkaAvroDeserializer::class.java.name
        kafkaProps["schema.registry.url"] = "http://localhost:8081"

        val c1 = KafkaTopicConsumer(kafkaProps, "UP_IOT_PROD_STREAM_MACHINE1", jedisPool)
        val c2 = KafkaTopicConsumer(kafkaProps, "UP_IOT_PROD_STREAM_MACHINE2", jedisPool)

        val executorService = Executors.newFixedThreadPool(2)
        executorService.submit(c1)
        executorService.submit(c2)

        val app = Javalin.create().start(7000)

        app.get("/") { ctx -> ctx.result("IoT data streams") }

        app.get("/test_machine_1") { ctx ->
            handleGetMachineData(ctx, "UP_IOT_PROD_STREAM_MACHINE1", jedisPool)
        }

        app.get("/test_machine_2") { ctx ->
            handleGetMachineData(ctx, "UP_IOT_PROD_STREAM_MACHINE2", jedisPool)
        }
    }


    private fun handleGetMachineData(ctx: Context, topic: String, jedisPool: JedisPool) {
        val top = java.lang.Long.parseLong(ctx.queryParam("top", "20"))
        val skip = java.lang.Long.parseLong(ctx.queryParam("skip", "0"))
        val json = StringBuilder()

        json.append("[")
        val jedisClient = jedisPool.resource
        jedisClient.use { jedis ->
            val records = jedis.lrange(topic, skip, top - 1)
            json.append(records.joinToString())
        }
        json.append("]")

        ctx.res.contentType = "application/json"
        ctx.result(json.toString())
    }
}