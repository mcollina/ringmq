'use strict'

const tap = require('tap')
const test = tap.test
const ringmq = require('.')
const cs = require('mqemitter-cs')
const net = require('net')

const first = ringmq({
  logLevel: 'error',
  interfaces: [{
    protocol: 'tcp',
    port: 3001
  }]
})

tap.tearDown(first.close.bind(first))

first.on('listening', function () {
  test('do pub sub', function (t) {
    t.plan(2)

    const client = cs.client(net.connect(3001))
    const msg = {
      topic: 'hello',
      payload: 'world'
    }

    t.tearDown(client.close.bind(client))

    client.on('hello', function (chunk, cb) {
      t.deepEqual(chunk, msg, 'msg received')
      cb()
    })

    client.emit(msg, function (err) {
      t.error(err, 'sent')
    })
  })

  test('do pub sub on another instance', function (t) {
    t.plan(2)

    const second = ringmq({
      logLevel: 'warn',
      base: [first.upring.whoami()],
      hashring: {
        joinTimeout: 200
      },
      interfaces: [{
        protocol: 'tcp',
        port: 3002
      }]
    })

    t.tearDown(second.close.bind(second))

    second.on('ringUp', function () {
      const client = cs.client(net.connect(3002))
      var topic = 'hello'

      // get a topic that is allocated to the first
      // instance
      while (second.upring.allocatedToMe(topic)) {
        topic += 'a'
      }

      const msg = {
        topic,
        payload: 'world'
      }

      t.tearDown(client.close.bind(client))

      client.on(msg.topic, function (chunk, cb) {
        t.deepEqual(chunk, msg, 'msg received')
        cb()
      })

      client.emit(msg, function (err) {
        t.error(err, 'sent')
      })
    })
  })

  test('do pub sub on another instance (reversed)', function (t) {
    t.plan(2)

    const second = ringmq({
      logLevel: 'warn',
      base: [first.upring.whoami()],
      hashring: {
        joinTimeout: 200
      },
      interfaces: [{
        protocol: 'tcp',
        port: 3002
      }]
    })

    t.tearDown(second.close.bind(second))

    second.on('ringUp', function () {
      const client = cs.client(net.connect(3002))
      var topic = 'hello'

      // get a topic that is allocated to the second
      // instance
      while (!second.upring.allocatedToMe(topic)) {
        topic += 'a'
      }

      const msg = {
        topic,
        payload: 'world'
      }

      t.tearDown(client.close.bind(client))

      client.on(msg.topic, function (chunk, cb) {
        t.deepEqual(chunk, msg, 'msg received')
        cb()
      })

      client.emit(msg, function (err) {
        t.error(err, 'sent')
      })
    })
  })

  test('do pub sub connected to two instances', function (t) {
    t.plan(2)

    const second = ringmq({
      logLevel: 'warn',
      base: [first.upring.whoami()],
      hashring: {
        joinTimeout: 200
      },
      interfaces: [{
        protocol: 'tcp',
        port: 3002
      }]
    })

    t.tearDown(second.close.bind(second))

    second.on('ringUp', function () {
      const client1 = cs.client(net.connect(3001))
      const client2 = cs.client(net.connect(3002))

      const msg = {
        topic: 'hello',
        payload: 'world'
      }

      t.tearDown(client1.close.bind(client1))
      t.tearDown(client2.close.bind(client2))

      client1.on(msg.topic, function (chunk, cb) {
        t.deepEqual(chunk, msg, 'msg received')
        cb()
      })

      client2.emit(msg, function (err) {
        t.error(err, 'sent')
      })
    })
  })
})
