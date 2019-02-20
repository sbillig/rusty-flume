'use strict'

const Obv = require('obv')
var rs = require('../native')

function id(x) { return x }

function encode_as_buffer(codec, s) {
  let e = codec.encode(s)
  return typeof e == 'string' ? Buffer.from(e) : e
}

function OffsetLog(filename, opts) {

  let codec = opts.codec || {encode: id, decode: id, buffer: true}

  let obv = Obv()
  let log = new rs.SyncLog(filename)
  obv.set(log.last_offset())

  return {
    filename: filename,
    since: obv,

    get: function(n, cb) {
      // TODO: error
      cb(null, codec.decode(log.get(n)))
    },

    since: obv,

    stream: function(opts) {
      opts = opts || {}

      // TODO: opts.reverse, .old, .live, .limit

      const remove_seqs = opts.seqs   === false
      const remove_vals = opts.values === false

      const stream_id = log.create_stream(opts.reverse === true)
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
          buf = log.read_from_stream(stream_id, 100)
          if (buf.length == 0) {
            // TODO: handle live streams
            cb(true, null)
            // TODO: destroy stream
            return
          }
        }

        let out = buf[buf_idx]
        buf[buf_idx++] = 0

        // `out` is { seq: <number>, value: <Buffer> }
        if (remove_seqs)
          cb(null, out.value)
        else if (remove_vals)
          cb(null, out.seq)
        else
          cb(null, out)
      }
    },

    append: function(val, cb) {
      var offset;
      if (val.constructor === Array) {
        let bufs = val.map((s) => encode_as_buffer(codec, s))
        offset = log.append_batch(bufs)
      } else {
        let buf = encode_as_buffer(codec, val)
        offset = log.append(buf)
      }
      obv.set(offset)
      cb(null, offset) // TODO: error
    }
  }
}

module.exports = {
  OffsetLog: OffsetLog
}
