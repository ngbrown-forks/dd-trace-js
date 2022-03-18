'use strict'

const {
  channel,
  addHook,
  AsyncResource
} = require('./helpers/instrument')
const shimmer = require('../../datadog-shimmer')

const producerStartCh = channel('apm:kafkajs:produce:start')
const producerMessageCh = channel(`apm:kafkajs:produce:message`)
const consumerStartCh = channel('apm:kafkajs:consume:start')

const asyncEndCh = channel('apm:kafkajs:async-end')
const endCh = channel('apm:kafkajs:end')
const errorCh = channel('apm:kafkajs:error')

addHook({ name: 'kafkajs', versions: ['>=1.4'] }, (obj) => {
  const Kafka = obj.Kafka
  shimmer.wrap(Kafka.prototype, 'producer', createProducer => function () {
    if (!producerStartCh.hasSubscribers) {
      return createProducer.apply(this, arguments)
    }
    const producer = createProducer.apply(this, arguments)
    const send = producer.send

    producer.send = wrapProducer(function (...args) {
      const { topic, messages = [] } = args[0]
      for (const message of messages) {
        message.headers = message.headers || {}
      }
      producerMessageCh.publish({ topic, messages })
      return send.apply(this, args)
    })
    return producer
  })

  shimmer.wrap(Kafka.prototype, 'consumer', createConsumer => function () {
    if (!consumerStartCh.hasSubscribers) {
      return createConsumer.apply(this, arguments)
    }

    const consumer = createConsumer.apply(this, arguments)
    const run = consumer.run

    consumer.run = function ({ eachMessage, ...runArgs }) {
      if (typeof eachMessage !== 'function') return run({ eachMessage, ...runArgs })

      return run({
        eachMessage: function (...eachMessageArgs) {
          const { topic, partition, message } = eachMessageArgs[0]
          consumerStartCh.publish({ topic, partition, message })
          return wrapRes(() => {
            return eachMessage.apply(this, eachMessageArgs)
          })
        },
        ...runArgs
      })
    }
    return consumer
  })
  return obj
})

function wrapProducer (fn) {
  return function () {
    const asyncResource = new AsyncResource('bound-anonymous-fn')
    producerStartCh.publish(undefined)
    const lastArgId = arguments.length - 1
    const cb = arguments[lastArgId]
    if (typeof cb === 'function') {
      const scopeBoundCb = asyncResource.bind(cb)
      arguments[lastArgId] = AsyncResource.bind(function (err) {
        if (err) {
          errorCh.publish(err)
        }
        asyncEndCh.publish(undefined)
        return scopeBoundCb.apply(this, arguments)
      })
    }
    return wrapRes(() => fn.apply(this, arguments))
  }
}

function wrapRes (fn) {
  try {
    if (fn.length > 1) {
      return AsyncResource.bind(() => fn(err => {
        errorCh.publish(err)
        asyncEndCh.publish(undefined)
      }))
    }

    const result = fn.apply(this, arguments)
    if (result && typeof result.then === 'function') {
      result.then(
        AsyncResource.bind(() => asyncEndCh.publish(undefined)),
        AsyncResource.bind(err => {
          errorCh.publish(err)
          asyncEndCh.publish(undefined)
        })
      )
    } else {
      asyncEndCh.publish(undefined)
    }

    return result
  } catch (e) {
    errorCh.publish(e)
    asyncEndCh.publish(undefined)
    throw e
  } finally {
    endCh.publish(undefined)
  }
}
