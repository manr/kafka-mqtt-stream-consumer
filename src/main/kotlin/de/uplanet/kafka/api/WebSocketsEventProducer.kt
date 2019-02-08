package de.uplanet.kafka.api

import io.javalin.websocket.WsSession
import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisPool
import redis.clients.jedis.JedisPubSub

class WebSocketsEventProducer(p_sessions: Map<WsSession, String>, p_jedisPool: JedisPool) : Runnable {
    private val sessions = p_sessions
    private val jedisPool = p_jedisPool

    override fun run() {
        val jedisClient = jedisPool.resource
        jedisClient.use { jedis ->
            jedis.subscribe(Subscriber(sessions, jedisPool), "TOPIC_UPDATES")
        }
    }

}

class Subscriber(private val sessions: Map<WsSession, String>, private val jedisPool: JedisPool)
    : JedisPubSub() {

    override fun onMessage(channel: String?, message: String?) {
        val topics = message?.split(",")
        val topicSet = topics?.toSet()

        if (channel == "TOPIC_UPDATES") {
            jedisPool.resource.use { jedis ->
                sessions.forEach { ws, topic ->
                    if (topicSet?.contains(topic) == true) {
                        val data = getMachineData(topic, 0, 1, jedis)
                        if (data != "")
                            ws.send(data)
                    }
                }
            }
        }
    }

    private fun getMachineData(topic: String, skip: Long, top: Long, jedis: Jedis) : String {
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