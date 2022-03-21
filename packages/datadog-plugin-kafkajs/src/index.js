'use strict'

const Plugin = require('../../dd-trace/src/plugins/plugin')
const { storage } = require('../../datadog-core')
const analyticsSampler = require('../../dd-trace/src/analytics_sampler')

class KafkajsPlugin extends Plugin {
  static get name () {
    return 'kafkajs'
  }

  constructor (...args) {
    super(...args)

    this.addSub(`apm:kafkajs:produce:start`, () => {
      const store = storage.getStore()
      const childOf = store ? store.span : store
      const span = this.tracer.startSpan('kafka.produce', {
        childOf,
        tags: {
          'service.name': this.config.service || `${this.tracer._service}-kafka`,
          'span.kind': 'producer',
          'component': 'kafkajs'
        }
      })

      analyticsSampler.sample(span, this.config.measured)
      this.enter(span, store)
    })

    this.addSub(`apm:kafkajs:produce:message`, ({ topic, messages }) => {
      const span = storage.getStore().span
      span.addTags({
        'resource.name': topic,
        'kafka.topic': topic,
        'kafka.batch_size': messages.length
      })
      for (const message of messages) {
        this.tracer.inject(span, 'text_map', message.headers)
      }
    })

    this.addSub(`apm:kafkajs:consume:start`, ({ topic, partition, message }) => {
      const store = storage.getStore()
      const childOf = extract(this.tracer, message.headers)
      const span = this.tracer.startSpan('kafka.consume', {
        childOf,
        tags: {
          'service.name': this.config.service || `${this.tracer._service}-kafka`,
          'span.kind': 'consumer',
          'span.type': 'worker',
          'component': 'kafkajs',
          'resource.name': topic,
          'kafka.topic': topic,
          'kafka.partition': partition,
          'kafka.message.offset': message.offset
        }
      })

      analyticsSampler.sample(span, this.config.measured, true)
      this.enter(span, store)
    })

    this.addSub(`apm:kafkajs:end`, () => {
      this.exit()
    })

    this.addSub(`apm:kafkajs:error`, err => {
      const span = storage.getStore().span
      span.setTag('error', err)
    })

    this.addSub(`apm:kafkajs:async-end`, () => {
      const span = storage.getStore().span
      span.finish()
    })
  }
}

function extract (tracer, bufferMap) {
  if (!bufferMap) return null

  const textMap = {}

  for (const key of Object.keys(bufferMap)) {
    textMap[key] = bufferMap[key].toString()
  }

  return tracer.extract('text_map', textMap)
}

module.exports = KafkajsPlugin
