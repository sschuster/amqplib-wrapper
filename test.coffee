amqp = require("./src/amqp-wrapper")
amqp.verbose = true

config = {
    host: [
        "localhost"
    ],
    port: 5672,
    user: "test",
    password: "test",
    vhost: "/test"
}

queue = new amqp.AmqpWrapper(config)

queue.assertExchange("mind42.fanout.config", "fanout", {
    durable: true,
    autoDelete: false
})

queue.assertExchange("mind42.direct.export", "direct", {
    durable: true,
    autoDelete: false
})

queue.assertQueue("mind42.queue.export", {
    durable: true,
    autoDelete: false
})

queue.assertBinding("mind42.queue.export", "mind42.direct.export", "export")

queue.subscribeQueue("mind42.queue.export", {
    prefetch: 5
}, (message) ->
    console.log("QUEUE", message.asJson())
    message.ack()
)

queue.subscribeExchange("testQueue", "mind42.fanout.config", "", {
    exclusive: true,
    noAck: true
}, (message) ->
    console.log("FANOUT1", message.asJson())
)

queue.subscribeExchange("testQueue2", "mind42.fanout.config", "", {
    exclusive: true,
    noAck: true
}, (message) ->
    console.log("FANOUT2", message.asJson())
)

setInterval(->
    queue.publish({ foo: "bar" }, "mind42.direct.export", "export", null, (err) ->
        if err
            console.log("Error sending message", err)
    )
, 2500)

setInterval(->
    queue.publish({ test: true }, "mind42.fanout.config", "", null, (err) ->
        if err
            console.log("Error sending message", err)
    )
, 5000)

setInterval(->
    console.log("CONFIG CHANGE")
    queue.updateConfig(config)
, 20000)
