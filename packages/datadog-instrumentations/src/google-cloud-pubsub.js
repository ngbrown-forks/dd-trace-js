'use strict'

const {
  channel,
  addHook,
  AsyncResource
} = require('./helpers/instrument')
const shimmer = require('../../datadog-shimmer')

const requestStartCh = channel('apm:google-cloud-pubsub:request:start')
const requestAddCh = channel(`apm:google-cloud-pubsub:request:add`)
const receiveStartCh = channel(`apm:google-cloud-pubsub:receive:start`)

const asyncEndCh = channel('apm:google-cloud-pubsub:async-end')
const endCh = channel('apm:google-cloud-pubsub:end')
const errorCh = channel('apm:google-cloud-pubsub:error')

const receiveAsyncEndCh = channel('apm:google-cloud-pubsub:receive:async-end')
const receiveEndCh = channel('apm:google-cloud-pubsub:receive:end')
const receiveErrorCh = channel('apm:google-cloud-pubsub:receive:error')

const messageSpans = new WeakMap()

addHook({ name: '@google-cloud/pubsub', versions: ['>=1.2'] }, (obj) => {
  const PubSub = obj.PubSub
  const Subscription = obj.Subscription

  shimmer.wrap(PubSub.prototype, 'request', request => function (cfg = { reqOpts: {} }, cb) {
    if (!requestStartCh.hasSubscribers) {
      return request.apply(this, arguments)
    }

    requestStartCh.publish({ cfg, projectId: this.projectId })

    const asyncResource = new AsyncResource('bound-anonymous-fn')

    cb = asyncResource.bind(cb)

    return wrapRes(() => {
      requestAddCh.publish({ cfg })
      arguments[1] = AsyncResource.bind(function (error) {
        if (error) {
          errorCh.publish(error)
        }
        asyncEndCh.publish(undefined)
        return cb.apply(this, arguments)
      })
      return request.apply(this, arguments)
    })
  })

  shimmer.wrap(Subscription.prototype, 'emit', emit => function (eventName, message) {
    if (eventName !== 'message' || !message) return emit.apply(this, arguments)

    const span = messageSpans.get(message)

    if (!span) return emit.apply(this, arguments)

    try {
      return emit.apply(this, arguments)
    } catch (err) {
      receiveErrorCh.publish({ err, message })

      throw err
    } finally {
      receiveEndCh.publish(undefined)
    }
  })

  return obj
})

addHook({ name: '@google-cloud/pubsub', versions: ['>=1.2'], file: 'build/src/lease-manager.js' }, (obj) => {
  const LeaseManager = obj.LeaseManager

  shimmer.wrap(LeaseManager.prototype, '_dispense', dispense => function (message) {
    if (!receiveStartCh.hasSubscribers) {
      return dispense.apply(this, arguments)
    }
    receiveStartCh.publish({ message })
    return dispense.apply(this, arguments)
  })

  shimmer.wrap(LeaseManager.prototype, 'remove', remove => function (message) {
    receiveAsyncEndCh.publish({ message })
    return remove.apply(this, arguments)
  })

  shimmer.wrap(LeaseManager.prototype, 'clear', clear => function () {
    for (const message of this._messages) {
      receiveAsyncEndCh.publish({ message })
    }
    return clear.apply(this, arguments)
  })

  return obj
})

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

module.exports = messageSpans
