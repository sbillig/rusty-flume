#[macro_use]
extern crate neon;
extern crate flumedb;

use flumedb::{BidirIterator, FlumeLog, LogEntry, OffsetLog, OffsetLogIter};
use neon::prelude::*;
use std::collections::HashMap;

pub struct SyncLog {
    log: flumedb::OffsetLog<u32>,
    streams: HashMap<u32, Stream>,
    next_id: u32
}

struct Stream {
    iter: OffsetLogIter<u32>,
    reverse: bool
}

declare_types! {
    pub class JsSyncLog for SyncLog {
        init(mut cx) {
            let path = cx.argument::<JsString>(0)?;

            Ok(SyncLog {
                log: OffsetLog::new(path.value()).unwrap(),
                streams: HashMap::new(),
                next_id: 0
            })
        }

        method last_offset(mut cx) {
            let n = {
                let mut this = cx.this();
                let g = cx.lock();
                let mut t = this.borrow_mut(&g);
                match t.log.latest() {
                    Some(n) => n as i64,
                    None    => -1
                }
            };
            Ok(cx.number(n as f64).upcast())
        }

        method get(mut cx) {
            let offset = cx.argument::<JsNumber>(0)?.value() as u64;

            let vec: Vec<u8> = {
                let mut this = cx.this();
                let g = cx.lock();
                let mut t = this.borrow_mut(&g);
                t.log.get(offset)
            }.unwrap();

            let mut buf = JsBuffer::new(&mut cx, vec.len() as u32).unwrap();
            cx.borrow_mut(&mut buf, |data| {
                let slice = data.as_mut_slice::<u8>();
                slice.copy_from_slice(&vec);
            });

            Ok(buf.upcast())
        }

        method append(mut cx) {
            let buf = cx.argument::<JsBuffer>(0)?;
            let r = {
                let mut this = cx.this();
                let g = cx.lock();
                let mut t = this.borrow_mut(&g);

                let data = buf.borrow(&g);
                t.log.append(data.as_slice::<u8>())
            }.unwrap();
            Ok(cx.number(r as f64).upcast())
        }

        method append_batch(mut cx) {
            // TODO: Should probably use t.log.append_batch
            let bufarray = cx.argument::<JsArray>(0)?;
            let bufvec = bufarray.to_vec(&mut cx)?;

            let r = {
                let mut this = cx.this();
                let g = cx.lock();
                let mut t = this.borrow_mut(&g);

                let mut offset = 0;
                for h in bufvec.iter() {
                    let buf = h.downcast::<JsBuffer>().unwrap();
                    let data = buf.borrow(&g);
                    offset = t.log.append(data.as_slice::<u8>()).unwrap();
                }
                offset
            };
            Ok(cx.number(r as f64).upcast())
        }


        method create_stream(mut cx) {
            let reverse = cx.argument::<JsBoolean>(0)?.value();

            let stream_id = {
                let mut this = cx.this();
                let g = cx.lock();
                let mut t = this.borrow_mut(&g);

                let id = t.next_id;
                t.next_id += 1;
                let iter = if reverse {
                    t.log.iter_at_offset(t.log.end())
                } else {
                    t.log.iter()
                };

                t.streams.insert(id, Stream { iter, reverse });
                id
            };

            Ok(cx.number(stream_id as f64).upcast())
        }

        method destroy_stream(mut cx) {
            let id = cx.argument::<JsNumber>(0)?.value() as u32;

            {
                let mut this = cx.this();
                let g = cx.lock();
                let mut t = this.borrow_mut(&g);
                t.streams.remove(&id);
            }

            Ok(cx.boolean(true).upcast())
        }

        method read_from_stream(mut cx) {
            let id = cx.argument::<JsNumber>(0)?.value() as u32;
            let count = cx.argument::<JsNumber>(1)?.value() as usize;

            // TODO: don't need this vec
            let entries: Vec<LogEntry> = {
                let mut this = cx.this();
                let g = cx.lock();
                let mut t = this.borrow_mut(&g);
                let mut stream = t.streams.get_mut(&id).unwrap();
                if stream.reverse {
                    stream.iter.backward().take(count).collect()
                } else {
                    stream.iter.forward().take(count).collect()
                }
            };

            let out = JsArray::new(&mut cx, entries.len() as u32);
            for (i, e) in entries.iter().enumerate() {
                let mut buf = cx.buffer(e.data.len() as u32).unwrap();
                cx.borrow_mut(&mut buf, |data| {
                    let slice = data.as_mut_slice::<u8>();
                    slice.copy_from_slice(&e.data);
                });
                let seq = cx.number(e.offset as f64);
                let entry = cx.empty_object();
                entry.set(&mut cx, "value", buf)?;
                entry.set(&mut cx, "seq", seq)?;
                out.set(&mut cx, i as u32, entry)?;
            }

            Ok(out.upcast())
        }

    }
}

fn hello(mut cx: FunctionContext) -> JsResult<JsString> {
    Ok(cx.string("hello node"))
}

register_module!(mut m, {
    m.export_function("hello", hello)?;
    m.export_class::<JsSyncLog>("SyncLog")?;
    Ok(())
});
