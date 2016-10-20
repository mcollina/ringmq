#!/usr/bin/env node
'use strict'

const fs = require('fs')
const path = require('path')
const net = require('net')
const UpringPubSub = require('upring-pubsub')
const cs = require('mqemitter-cs')
const minimist = require('minimist')
const helpMe = require('help-me')
const factory = require('server-factory')
const commist = require('commist')

function ringmq (opts) {
  opts = opts || {}
  if (!opts.interfaces) {
    throw new Error('opts.interfaces is needed')
  }
  const servers = factory(opts.interfaces)
  servers.mq = new UpringPubSub(opts)
  servers.upring = servers.mq.upring
  servers.logger = servers.upring.logger
  servers.on('stream', cs.server(servers.mq))

  const oldClose = servers.close

  servers.close = function (cb) {
    cb = cb || noop
    oldClose.call(this, function (err) {
      servers.upring.close(function (err2) {
        if (err) {
          return cb(err)
        }

        if (err2) {
          return cb(err2)
        }

        cb()
      })
    })
  }

  servers.on('listening', function () {
    servers.logger.info({ addresses: servers.addresses() }, 'listening')
  })
  servers.upring.on('up', servers.emit.bind(servers, 'ringUp'))

  return servers
}

function noop () {}

module.exports = ringmq

function cli (args) {
  const program = commist()

  program.register('start', function (args) {
    var parsed = minimist(args, {
      alias: {
        'p': 'port',
        'b': 'base',
        'c': 'config'
      }
    })
    var bases = parsed.base || parsed._

    if (parsed.config) {
      const config = JSON.parse(fs.readFileSync(path.resolve(parsed.config)))
      config.base = bases.length ? bases : config.base
      if (config.interfaces.length === 0) {
        console.error('missing interfaces')
        process.exit(1)
      }
      ringmq(config)
    } else {
      console.log('Error: --config file needed\n')
      help()
    }
  })

  program.register('publish', function (args) {
    var parsed = minimist(args, {
      default: {
        host: 'localhost'
      },
      alias: {
        'p': 'port',
        'h': 'host',
        't': 'topic',
        'm': 'message'
      }
    })

    if (!parsed.port) {
      console.error('missing port\n')
      help('publish')
      return
    }

    if (!parsed.topic) {
      console.error('missing topic\n')
      help('publish')
      return
    }

    const stream = net.connect(parsed.port, parsed.host)
    stream.on('error', console.log)
    const client = cs.client(stream)
    client.emit({ topic: parsed.topic, message: parsed.message }, function (err) {
      if (err) {
        throw err
      }

      client.close()
    })
  })

  program.register('subscribe', function (args) {
    var parsed = minimist(args, {
      default: {
        host: 'localhost'
      },
      alias: {
        'p': 'port',
        'h': 'host',
        't': 'topic'
      }
    })

    if (!parsed.port) {
      console.error('missing port\n')
      help('subscribe')
      return
    }

    const client = cs.client(net.connect(parsed.port, parsed.host))
    client.on(parsed.topic, function (msg, cb) {
      console.log(msg)
      cb()
    }, function (err) {
      if (err) {
        throw err
      }
    })
  })

  program.register('version', function (args) {
    console.log('ringmq', 'v' + require('./package.json').version)
    console.log('node', process.version)
  })

  program.register('help', help)

  const parsed = program.parse(args)

  if (parsed !== null) {
    if (parsed.length > 0) {
      console.log('No such command:', args[0], '\n')
    }
    help(['help'])
  }

  function help (args) {
    helpMe({
      // the default
      dir: path.join(path.dirname(require.main.filename), 'helps'),
      // the default
      ext: '.txt'
    }).toStdout(args)
  }
}

if (require.main === module) {
  cli(process.argv.splice(2))
}
