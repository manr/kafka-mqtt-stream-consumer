package de.uplanet.kafka.api

import io.javalin.websocket.WsSession
import redis.clients.jedis.JedisPool

class WebSocketsEventProducer(p_sessions: Map<WsSession, String>, p_jedisPool: JedisPool) : Runnable {
    private val sessions = p_sessions
    private val jedisPool = p_jedisPool

    override fun run() {
        while (true) {
            sessions.forEach { ws, topic ->
                val data = getMachineData(topic, 0, 1, jedisPool)
                if (data != "")
                    ws.send(data)
            }
            Thread.sleep(1000)
        }
    }

    private fun getMachineData(topic: String, skip: Long, top: Long, jedisPool: JedisPool) : String {
        val jedisClient = jedisPool.resource
        jedisClient.use { jedis ->
            val records = jedis.lrange(topic, skip, top - 1)

            return if (records.size > 0) {
                val json = StringBuilder()
                json.append("{\"topic\": \"$topic\", \"stream\": ")
                json.append("[")
                json.append(records.joinToString())
                json.append("]")
                json.append("}")
                json.toString()
            } else {
                ""
            }
        }
    }
}