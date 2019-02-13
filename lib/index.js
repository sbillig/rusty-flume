'use strict'

function id(x) { return x }

const Obv = require('obv')
var rs = require('../native')

function create_log(filename, opts) {

  let codec = opts.codec || {encode: id, decode: id, buffer: true}

  let obv = Obv()
  let log = new rs.SyncLog(filename)

  return {
    filename: filename,
    since: obv,

    get: function(n, cb) {
      // TODO: error
      cb(null, codec.decode(log.get(n)))
    },

    // since: since,

    stream: function(opts) {
      // TODO: handle opts
      const stream_id = log.create_stream()
      let buf = []
      let buf_idx = 0
      // const is_live = opts.live

      return function next(quit, cb) {
        if (quit) {
          log.destroy_stream(stream_id)
          cb(quit, null)
          return
        }

        if (buf_idx >= buf.length) {
          buf_idx = 0
          buf = log.read_from_stream(stream_id, 10)
          if (buf.length == 0) {
            // TODO: handle live streams
            cb(true, null)
            return
          }
        }

        let out = buf[buf_idx]
        buf[buf_idx++] = 0

        cb(null, out)
      }
    },

    append: function(val, cb) {
      let offset = log.append(codec.encode(val))
      cb(null, offset) // TODO: error
    }
  }

}

module.exports = create_log

var file = '/tmp/offset-test_'+Date.now()+'.log'
let log = create_log(file, {});

log.append(Buffer.from('abc'), function (err, offset) {
  console.log('offset 0:', offset)
})

log.append(Buffer.from('123'), function (err, offset) {
  console.log('offset 1:', offset)
})

let r = log.get(0, function(err, buf){
  console.log(buf.toString('utf8'))
});

let s = log.stream()
s(false, (err, buf) => { console.log(buf.toString('utf8')) })
s(false, (err, buf) => { console.log(buf.toString('utf8')) })
