amqplib = require("amqplib")
whenlib = require("when")
{ExchangeAssertion, QueueAssertion, BindingAssertion} = require("./assertion")
{QueueSubscription, ExchangeSubscription} = require("./subscription")

module.exports.reconnectTimeout = 1000

module.exports.AmqpWrapper = class AmqpWrapper
    constructor: (config) ->
        @readyPromise = null
        @clientPromise = null
        @sharedChannelPromise = null
        @config = null
        @assertions = []
        @subscriptions = {}

        @updateConfig(config)

    updateConfig: (config) ->
        if config
            @config = config

        #Destroy existing client
        if @clientPromise
            @disconnect()
        #Create new client
        else
            @readyPromise = whenlib.promise((resolve, reject, notify) =>
                host = @config.host[Math.floor(Math.random() * @config.host.length)] #Pick random host
                amqpUrl = "amqp://#{@config.user}:#{@config.password}@#{host}:#{@config.port}/#{encodeURIComponent(@config.vhost)}"

                @clientPromise = amqplib.connect(amqpUrl)
                @clientPromise.then((client) =>
                    #Setup client
                    client.on("close", =>
                        setTimeout(=>
                            @clientPromise = null
                            @updateConfig()
                        , module.exports.reconnectTimeout)
                    )
                    client.on("error", ->)

                    #Create shared channel and setup error handle
                    @sharedChannelPromise = client.createChannel()
                    @sharedChannelPromise.then((channel) =>
                        #Setup channel
                        channel.on("error", (err) =>
                            @disconnect()
                        )

                        #Init
                        whenlib.all((assertion.execute(channel) for assertion in @assertions)).then(=>
                            for queueName, subscription of @subscriptions
                                subscription.setClient(client)
                            resolve(channel);
                        ).otherwise((err) =>
                            setTimeout(=>
                                @disconnect()
                            , module.exports.reconnectTimeout)
                            reject(err)
                        )

                    ).otherwise((err) =>
                        setTimeout(=>
                            @disconnect()
                        , module.exports.reconnectTimeout)
                        reject(err)
                    )

                ).otherwise((err) =>
                    setTimeout(=>
                        @clientPromise = null
                        @updateConfig()
                    , module.exports.reconnectTimeout)
                    reject(err)
                )
            )

    disconnect: ->
        if @clientPromise
            @clientPromise.then((client) =>
                #Close all channels then close client
                closePromises = (subscription.disconnect() for queueName, subscription of @subscriptions)
                closePromises.push(whenlib(@sharedChannelPromise, (channel) ->
                    return channel.close()
                ))
                @sharedChannelPromise = null
                whenlib.all(closePromises).ensure(->
                    client.close()
                )
            )
            @clientPromise = null

    shutdown: ->
        @updateConfig = ->
        @disconnect()

    assertExchange: (name, type, options) ->
        assertion = new ExchangeAssertion(name, type, options)
        @assertions.push(assertion)

        if @sharedChannelPromise
            assertion.execute(@sharedChannelPromise)

    assertQueue: (name, options) ->
        assertion = new QueueAssertion(name, options)
        @assertions.push(assertion)

        if @sharedChannelPromise
            assertion.execute(@sharedChannelPromise)

    assertBinding: (queue, exchange, routingKey) ->
        assertion = new BindingAssertion(queue, exchange, routingKey)
        @assertions.push(assertion)

        if @sharedChannelPromise
            assertion.execute(@sharedChannelPromise)

    publish: (msg, exchange, routingKey, options, cb) ->
        try
            if msg not instanceof Buffer
                if (typeof msg) != "string"
                    msg = JSON.stringify(msg)
                msg = new Buffer(msg)
        catch err
            if cb
                cb(new Error("amqp-wrapper.publish couldn't serialize msg"))
            return 

        @readyPromise.then((channel) =>
            if channel.publish(exchange, routingKey, msg, options)
                if cb
                    cb(null)
            else
                if cb
                    cb(new Error("amqp-wrapper.publish encountered a full queue buffer"))
        ).otherwise((err) ->
            if cb
                cb(err)
        )

    subscribeQueue: (queueName, options, subscriberCb) ->
        if queueName not of @subscriptions
            @subscriptions[queueName] = new QueueSubscription(queueName, options)
            if @clientPromise
                @clientPromise.then((client) =>
                    @subscriptions[queueName].setClient(client)
                )
        return @subscriptions[queueName].subscribe(subscriberCb)

    subscribeExchange: (queueName, exchangeName, routingKey, options, subscriberCb) ->
        if queueName not of @subscriptions
            @subscriptions[queueName] = new ExchangeSubscription(queueName, exchangeName, routingKey, options)
            if @clientPromise
                @clientPromise.then((client) =>
                    @subscriptions[queueName].setClient(client)
                )
        return @subscriptions[queueName].subscribe(subscriberCb)
