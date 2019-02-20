var flume = require('./lib/')
var codec = require('flumecodec')

require('bench-flumelog')(function () {
  return flume.OffsetLog('/tmp/bench-flumelog-offset' + Date.now(), {
    codec: codec.json
  })
}, null, null, function (obj) {
  return obj
  //  return Buffer.from(JSON.stringify(obj))
})
