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
        val cfg = JedisPoolConfig()
        cfg.maxTotal = 10

        val jedisPool = JedisPool(cfg, "localhost")

        val kafkaProps = Properties()
        kafkaProps["bootstrap.servers"] = "localhost:29092"
        kafkaProps["group.id"] = "consumer-tutorial"
        kafkaProps["auto.offset.reset"] = "latest"
        kafkaProps["key.deserializer"] = StringDeserializer::class.java.name
        kafkaProps["value.deserializer"] = KafkaAvroDeserializer::class.java.name
        kafkaProps["schema.registry.url"] = "http://localhost:8081"

        val c1 = KafkaTopicConsumer(kafkaProps, "UP_IOT_PROD_STREAM_MACHINE1", 999, jedisPool)
        val c2 = KafkaTopicConsumer(kafkaProps, "UP_IOT_PROD_STREAM_MACHINE2", 999, jedisPool)
        val c3 = KafkaTopicConsumer(kafkaProps, "UP_IOT_PROD_TOTAL_MACHINE1", 0, jedisPool)
        val c4 = KafkaTopicConsumer(kafkaProps, "UP_IOT_PROD_TOTAL_MACHINE2", 0, jedisPool)
        val c5 = KafkaTopicConsumer(kafkaProps, "UP_IOT_PROD_STREAM_STATUS_MACHINE1", 0, jedisPool)
        val c6 = KafkaTopicConsumer(kafkaProps, "UP_IOT_PROD_STREAM_STATUS_MACHINE2", 0, jedisPool)
        val c7 = KafkaTopicConsumer(kafkaProps, "UP_IOT_PROD_QUALITY_MACHINE1", 0, jedisPool)
        val c8 = KafkaTopicConsumer(kafkaProps, "UP_IOT_PROD_QUALITY_MACHINE2", 0, jedisPool)

        val executorService = Executors.newCachedThreadPool()
        executorService.submit(c1)
        executorService.submit(c2)
        executorService.submit(c3)
        executorService.submit(c4)
        executorService.submit(c5)
        executorService.submit(c6)
        executorService.submit(c7)
        executorService.submit(c8)

        val app = Javalin.create().start(7000)

        app.get("/") { ctx ->
            val machines = listOf(mapOf(Pair("name", "test_machine_1"),
                                        Pair("prodUrl", "/test_machine_1"),
                                        Pair("statusUrl", "/test_machine_1/status"),
                                        Pair("totalUrl", "/test_machine_1/total")),
                                  mapOf(Pair("name", "test_machine_2"),
                                        Pair("prodUrl", "/test_machine_2"),
                                        Pair("statusUrl", "/test_machine_2/status"),
                                        Pair("totalUrl", "/test_machine_2/total")))
            ctx.json(machines)
        }

        app.get("/test_machine_1") { ctx ->
            handleGetMachineData(ctx, "UP_IOT_PROD_STREAM_MACHINE1", jedisPool)
        }

        app.get("/test_machine_2") { ctx ->
            handleGetMachineData(ctx, "UP_IOT_PROD_STREAM_MACHINE2", jedisPool)
        }

        app.get("/test_machine_1/total") { ctx ->
            handleGetMachineData(ctx, "UP_IOT_PROD_TOTAL_MACHINE1", jedisPool)
        }

        app.get("/test_machine_2/total") { ctx ->
            handleGetMachineData(ctx, "UP_IOT_PROD_TOTAL_MACHINE2", jedisPool)
        }

        app.get("/test_machine_1/status") { ctx ->
            handleGetMachineData(ctx, "UP_IOT_PROD_STREAM_STATUS_MACHINE1", jedisPool)
        }

        app.get("/test_machine_2/status") { ctx ->
            handleGetMachineData(ctx, "UP_IOT_PROD_STREAM_STATUS_MACHINE2", jedisPool)
        }

        app.get("/test_machine_1/quality") { ctx ->
            handleGetMachineData(ctx, "UP_IOT_PROD_QUALITY_MACHINE1", jedisPool)
        }

        app.get("/test_machine_2/quality") { ctx ->
            handleGetMachineData(ctx, "UP_IOT_PROD_QUALITY_MACHINE2", jedisPool)
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