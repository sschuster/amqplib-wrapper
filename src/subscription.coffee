whenlib = require("when")

class Subscription
    constructor: (options) ->
        @options = options
        @client = null
        @channelPromise = null
        @subscribers = []

    setClient: (client) ->
        @client = client
        if @subscribers.length > 0
            @connect()

    connect: ->
        if not @client
            return
        if @channelPromise
            @disconnect()

        @channelPromise = @client.createChannel()
        @channelPromise.then((channel) =>
            if "prefetch" of @options
                channel.prefetch(5)
        )

    disconnect: ->
        promise = null

        if @channelPromise
            promise = whenlib(@channelPromise, (channel) ->
                return channel.close()
            )
            @channelPromise = null
        else
            promise = whenlib.promise((resolve, reject, notify) =>
                resolve(true)
            )

        return promise

    subscribe: (subscriberCb) ->
        @subscribers.push(subscriberCb)
        
        if not @channelPromise
            @connect()

        return {
            remove: =>
                @subscribers = (subscriber for subscriber in @subscribers when subscriber != subscriberCb)
                if @subscribers.length == 0
                    @disconnect()
        }

class Message
    constructor: (channel, rawMessage) ->
        @channel = channel
        @rawMessage = rawMessage

    ack: ->
        @channel.ack(@rawMessage)

    asString: ->
        return @rawMessage.content.toString()

    asJson: ->
        return JSON.parse(@asString())

module.exports.QueueSubscription = class QueueSubscription extends Subscription
    constructor: (queueName, options) ->
        super(options)

        @queueName = queueName

    connect: ->
        super()
        
        if not @channelPromise
            return

        @channelPromise.then((channel) =>
            channel.consume(@queueName, (rawMessage) =>
                message = new Message(channel, rawMessage)
                for subscriber in @subscribers
                    subscriber(message)
            , @options)
        )

module.exports.ExchangeSubscription = class ExchangeSubscription extends Subscription
    constructor: (queueName, exchangeName, routingKey, options) ->
        super(options)

        @queueName = queueName
        @exchangeName = exchangeName
        @routingKey = routingKey

    connect: ->
        super()

        if not @channelPromise
            return

        @channelPromise.then((channel) =>
            channel.assertQueue(@queueName, {
                durable: false,
                exclusive: true,
                autoDelete: true
            })
            channel.bindQueue(@queueName, @exchangeName, @routingKey)
            channel.consume(@queueName, (rawMessage) =>
                message = new Message(channel, rawMessage)
                for subscriber in @subscribers
                    subscriber(message)
            , @options)
        )
