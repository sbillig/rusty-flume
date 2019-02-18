let test = require('tape')
let run_flumelog_tests = require('test-flumelog')
let OffsetLog = require('../').OffsetLog

function tmpfile() {
  return '/tmp/offset-test_'+Date.now()+'.log'
}

test('basics', (t) => {
  let log = OffsetLog(tmpfile(), {})
  t.equal(log.since.value, -1, 'empty log .since.value == -1')

  log.append(Buffer.from('abc'), (err, offset) => {
    t.equal(offset, 0, 'initial offset is 0')

    log.append(Buffer.from('12345'), (err, offset) => {
      t.equal(offset, 15, 'next offset') // three u32s + "abc"
      t.equal(log.since.value, 15, '.since.value is latest offset')

      log.get(15, function(err, buf) {
        t.equal(buf.toString('utf8'), '12345', 'get second record')
      })

      let s = log.stream()
      s(false, (err, entry) => {
        t.equal(entry.seq, 0, 'stream 0 seq')
        t.equal(entry.value.toString('utf8'), 'abc', 'stream 0 value')

        s(false, (err, entry) => {
          t.equal(entry.seq, 15, 'stream 1 seq')
          t.equal(entry.value.toString('utf8'), '12345', 'stream 1')

          s(false, (err, entry) => {
            t.equal(err, true, 'stream end')
            t.end()
          })
        })
      })
    })
  })
})

test('foo', (t) => {
  let log = OffsetLog(tmpfile(), {})
  log.append(['abc', 'def', '123', '456'], (err, offset) => {
    t.equal(offset, 45, 'append batch')

    let s = log.stream({reverse: true, values: false})
    s(false, (err, seq) => {
      t.equal(seq, 45, 'reverse stream')
      s(false, (err, seq) => {
        t.equal(seq, 30, 'reverse stream')
        t.end()
      })
    })

  })
})


// run_flumelog_tests(() => OffsetLog(tmpfile(), {}),
//                    () => console.log('done'))
