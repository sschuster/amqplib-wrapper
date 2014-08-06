whenlib = require("when")

module.exports.ExchangeAssertion = class ExchangeAssertion
    constructor: (name, type, options) ->
        @name = name
        @type = type
        @options = options

    execute: (channelOrPromise) ->
        return whenlib(channelOrPromise, (channel) =>
            return channel.assertExchange(@name, @type, @options)
        )

module.exports.QueueAssertion = class QueueAssertion
    constructor: (name, options) ->
        @name = name
        @options = options

    execute: (channelOrPromise) ->
        return whenlib(channelOrPromise, (channel) =>
            return channel.assertQueue(@name, @options)
        )

module.exports.BindingAssertion = class BindingAssertion
    constructor: (queue, exchange, routingKey) ->
        @queue = queue
        @exchange = exchange
        @routingKey = routingKey

    execute: (channelOrPromise) ->
        return whenlib(channelOrPromise, (channel) =>
            return channel.bindQueue(@queue, @exchange, @routingKey)
        )
