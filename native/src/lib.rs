#[macro_use]
extern crate neon;
extern crate flumedb;

use flumedb::{FlumeLog, LogEntry, OffsetLog, OffsetLogIter};
use neon::prelude::*;
use std::collections::HashMap;

pub struct SyncLog {
    log: flumedb::OffsetLog<u32>,
    streams: HashMap<u32, OffsetLogIter<u32>>,
    next_id: u32
}

declare_types! {
    pub class JsSyncLog for SyncLog {
        init(mut cx) {
            let path = cx.argument::<JsString>(0)?;

            Ok(SyncLog {
                log: OffsetLog::new(path.value()),
                streams: HashMap::new(),
                next_id: 0
            })
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

        method create_stream(mut cx) {
            let stream_id = {
                let mut this = cx.this();
                let g = cx.lock();

                let mut t = this.borrow_mut(&g);

                let id = t.next_id;
                t.next_id += 1;
                let iter = t.log.iter();

                t.streams.insert(id, iter);
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
            let count = cx.argument::<JsNumber>(1)?.value() as u32;

            let out = JsArray::new(&mut cx, count);
            let entries: Vec<LogEntry> = {
                let mut this = cx.this();
                let g = cx.lock();
                let mut t = this.borrow_mut(&g);
                t.streams.get_mut(&id).map(|iter| iter.take(count as usize).collect()).unwrap()
            };
            for (i, e) in entries.iter().enumerate() {
                let mut buf = JsBuffer::new(&mut cx, e.data_buffer.len() as u32).unwrap();
                cx.borrow_mut(&mut buf, |data| {
                    let slice = data.as_mut_slice::<u8>();
                    slice.copy_from_slice(&e.data_buffer);
                });
                out.set(&mut cx, i as u32, buf)?;
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
