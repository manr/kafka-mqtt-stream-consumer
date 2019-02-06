package de.uplanet.kafka.api

import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.javalin.Context
import io.javalin.Javalin
import io.javalin.websocket.WsSession
import org.apache.kafka.common.serialization.StringDeserializer
import redis.clients.jedis.JedisPool
import redis.clients.jedis.JedisPoolConfig
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors


object Main {
    private val wsSessions = ConcurrentHashMap<WsSession, String>()

    @JvmStatic
    fun main(args: Array<String>) {
        val cfg = JedisPoolConfig()
        cfg.maxTotal = 20

        val jedisPool = JedisPool(cfg, "localhost")

        val kafkaProps = Properties()
        kafkaProps["bootstrap.servers"] = "localhost:29092"
        kafkaProps["group.id"] = "consumer-tutorial"
        kafkaProps["auto.offset.reset"] = "latest"
        kafkaProps["key.deserializer"] = StringDeserializer::class.java.name
        kafkaProps["value.deserializer"] = KafkaAvroDeserializer::class.java.name
        kafkaProps["schema.registry.url"] = "http://localhost:8081"

        val c1 = KafkaTopicConsumer(kafkaProps, "STREAM_UP_IOT_PRODUCTION_MACHINE1", 999, jedisPool)
        val c2 = KafkaTopicConsumer(kafkaProps, "STREAM_UP_IOT_PRODUCTION_MACHINE2", 999, jedisPool)
        val c3 = KafkaTopicConsumer(kafkaProps, "STREAM_UP_IOT_PROD_TOTAL_DAILY_MACHINE1", 0, jedisPool)
        val c4 = KafkaTopicConsumer(kafkaProps, "STREAM_UP_IOT_PROD_TOTAL_DAILY_MACHINE2", 0, jedisPool)
        val c5 = KafkaTopicConsumer(kafkaProps, "STREAM_UP_IOT_PROD_TOTAL_HOURLY_MACHINE1", 0, jedisPool)
        val c6 = KafkaTopicConsumer(kafkaProps, "STREAM_UP_IOT_PROD_TOTAL_HOURLY_MACHINE2", 0, jedisPool)
        val c7 = KafkaTopicConsumer(kafkaProps, "STREAM_UP_IOT_PROD_QUALITY_MACHINE1", 0, jedisPool)
        val c8 = KafkaTopicConsumer(kafkaProps, "STREAM_UP_IOT_PROD_QUALITY_MACHINE2", 0, jedisPool)

        val executorService = Executors.newCachedThreadPool()
        executorService.submit(c1)
        executorService.submit(c2)
        executorService.submit(c3)
        executorService.submit(c4)
        executorService.submit(c5)
        executorService.submit(c6)
        executorService.submit(c7)
        executorService.submit(c8)

        val wsProducer = WebSocketsEventProducer(wsSessions, jedisPool)
        executorService.submit(wsProducer)

        val app = Javalin.create().start(8080)

        app.get("/") { ctx ->
            val machines = listOf(mapOf(Pair("name", "test_machine_1"),
                                        Pair("prodUrl", "/machine/STREAM_UP_IOT_PRODUCTION_MACHINE1"),
                                        Pair("totalUrl", "/machine/STREAM_UP_IOT_PROD_TOTAL_DAILY_MACHINE1"),
                                        Pair("qualityUrl", "/machine/STREAM_UP_IOT_PROD_QUALITY_MACHINE1")),
                                  mapOf(Pair("name", "test_machine_2"),
                                        Pair("prodUrl", "/machine/STREAM_UP_IOT_PRODUCTION_MACHINE2"),
                                        Pair("totalUrl", "/machine/STREAM_UP_IOT_PROD_TOTAL_DAILY_MACHINE2"),
                                        Pair("qualityUrl","/machine/STREAM_UP_IOT_PROD_QUALITY_MACHINE2")))
            ctx.json(machines)
        }

        app.get("/machine/:topic") { ctx ->
            handleGetMachineData(ctx, ctx.pathParam("topic"), jedisPool)
        }


        app.get("/ws/stream") { ctx ->
            ctx.html(wsIndex)
        }

        app.ws("/ws/stream/:machine") { ws ->
            ws.onConnect { session ->
                wsSessions[session] = session.pathParam("machine")
                println("WS connected")
            }
            ws.onMessage { session, message ->
                println("Received: $message")
                session.remote.sendString("Echo: $message")
            }
            ws.onClose { session, _, _ ->
                wsSessions.remove(session)
                println("Closed: ${session.id}")
            }
            ws.onError { _, throwable -> println("Error ${throwable?.message}") }
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

    private val wsIndex = """<!DOCTYPE HTML>
    <html>
       <head>

          <script type = "text/javascript">
             var ws;
             function StopWebSocketTest() {
                ws.close();
             }

             function WebSocketTest() {

                if ("WebSocket" in window) {
                   //alert("WebSocket is supported by your Browser!");

                   // Let us open a web socket
                   ws = new WebSocket("ws://upiotstreams.westeurope.cloudapp.azure.com:8080/ws/stream/" + document.getElementById('topic').value);

                   ws.onopen = function() {

                      // Web Socket is connected, send data using send()
                      console.log("Connection established...");
                      //ws.send("Message to send");

                      var div = document.getElementById('divID');

                      div.innerHTML = '';
                   };

                   ws.onmessage = function (evt) {
                      var received_msg = evt.data;
                      //alert("Message is received..." + evt.data);
                      console.log(evt.data)
                      
                      var div = document.getElementById('divID');

                      div.innerHTML += evt.data+'<br/>';
                   };

                   ws.onclose = function() {

                      // websocket is closed.
                      console.log("Connection is closed...");
                   };
                } else {

                   // The browser doesn't support WebSocket
                   alert("WebSocket NOT supported by your Browser!");
                }
             }
          </script>

       </head>

       <body>
          <div id = "sse">
             <select id="topic">
                  <option value="STREAM_UP_IOT_PRODUCTION_MACHINE1">STREAM_UP_IOT_PRODUCTION_MACHINE1</option>

                <option value="STREAM_UP_IOT_PRODUCTION_MACHINE2">STREAM_UP_IOT_PRODUCTION_MACHINE2</option>

                <option value="STREAM_UP_IOT_PROD_TOTAL_DAILY_MACHINE1">STREAM_UP_IOT_PROD_TOTAL_DAILY_MACHINE1</option>

                <option value="STREAM_UP_IOT_PROD_TOTAL_DAILY_MACHINE2">STREAM_UP_IOT_PROD_TOTAL_DAILY_MACHINE2</option>

                <option value="STREAM_UP_IOT_PROD_TOTAL_HOURLY_MACHINE1">STREAM_UP_IOT_PROD_TOTAL_HOURLY_MACHINE1</option>

                <option value="STREAM_UP_IOT_PROD_TOTAL_HOURLY_MACHINE2">STREAM_UP_IOT_PROD_TOTAL_HOURLY_MACHINE2</option>

                <option value="STREAM_UP_IOT_PROD_QUALITY_MACHINE1">STREAM_UP_IOT_PROD_QUALITY_MACHINE1</option>

                <option value="STREAM_UP_IOT_PROD_QUALITY_MACHINE2">STREAM_UP_IOT_PROD_QUALITY_MACHINE2</option>
              </select>
              <br/>
             <a href = "javascript:WebSocketTest()">Run WebSocket</a><br/>
             <a href = "javascript:StopWebSocketTest()">Stop WebSocket</a>
          </div>
          <div id="divID"></div>
       </body>
    </html>"""
}
