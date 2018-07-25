extern crate timely;
extern crate differential_dataflow;
extern crate actix;
extern crate actix_web;
extern crate futures;
extern crate timely_viz;

extern crate abomonation;
#[macro_use] extern crate abomonation_derive;

extern crate serde;
#[macro_use] extern crate serde_derive;
extern crate serde_json;

use std::sync::{Arc, Mutex};

use timely::dataflow::operators::{Map, capture::Replay};
use timely::progress::timestamp::RootTimestamp;
use timely::logging::TimelyEvent::{Operates, Schedule, Channels, Messages};
use timely::dataflow::operators::{Operator, Concat};

use differential_dataflow::AsCollection;
use differential_dataflow::operators::{Count, Consolidate};

use actix::*;
use actix_web::*;
use actix_web::{App, HttpRequest, Result, http::Method, fs::NamedFile};

use futures::future::Future;

// ==== Web-server setup ====
use timely_viz::server::Ws;

// #[derive(Serialize, Clone, Ord, PartialOrd, Eq, PartialEq)]
// struct Update {
//     data: String,   // name of data source.
//     name: String,
//     addr: String,
//     parent_addr: String,
//     size: isize,
//     diff: isize,
// }


#[derive(Serialize, Abomonation, Clone, PartialOrd, PartialEq, Eq, Ord, Debug)]
struct Update {
    event: Event,
    delta: isize,
}

#[derive(Serialize, Abomonation, Clone, PartialOrd, PartialEq, Eq, Ord, Debug)]
enum Event {
    // Largely static
    Operate {
        id: usize,              // unique identifier
        addr: Vec<usize>,       // (x, .., y, z)
        name: String,           // Helpful name
    },
    // Largely static
    Channel {
        id: usize,              // unique identifier
        from_addr: Vec<usize>,  // (x, .., y, z1)
        from_port: usize,       // port1
        to_addr: Vec<usize>,    // (x, .., y, z2)
        to_port: usize,         // port2
    },
    Schedule {
        operate_id: usize,
        elapsed_ns: isize,      // total elapsed ns
    },
    Messages {
        channel_id: usize,
        total_count: isize,     // total typed records
    }
}


#[derive(Serialize, Clone)]
struct Updates {
    updates: Vec<Update>,
}

impl actix::Message for Updates {
    type Result = ();
}

impl Handler<Updates> for Ws {
    type Result = ();

    fn handle(&mut self, msg: Updates, ctx: &mut Self::Context) {
        let text = serde_json::to_string(&msg).unwrap();
        // println!("Sending: {}", text);
        ctx.text(text);
    }
}
// ==========================

fn main() {
    let endpoint: Arc<Mutex<Vec<Addr<Syn, Ws>>>> = Arc::new(Mutex::new(Vec::new()));
    let endpoint_actix = endpoint.clone();

    ::std::mem::drop(::std::thread::spawn(move || {

    let mut args = ::std::env::args();
    args.next().unwrap();

    // the number of workers in the computation we're examining
    let source_peers = args.next().expect("Must provide number of source peers").parse::<usize>().expect("Source peers must be an unsigned integer");
    let sockets = timely_viz::open_sockets(source_peers);

    timely::execute_from_args(std::env::args(), move |worker| {

        let endpoint = endpoint.clone();

        let sockets = sockets.clone();

        // create replayers from disjoint partition of source worker identifiers
        let replayers = timely_viz::make_replayers(sockets, worker.index(), worker.peers());

        let shift = 25;

        worker.dataflow::<u64,_,_>(|scope| {

            let replayed = replayers.replay_into(scope);

            let operates =
            replayed
                .flat_map(move |(ts, _setup, datum)|
                    if let Operates(event) = datum {
                        let ts = ((ts >> shift) + 1) << shift;
                        Some((event, RootTimestamp::new(ts), 1))
                    }
                    else {
                        None
                    }
                )
                .as_collection()
                .map(|x| (x.id, x.addr, x.name))
                .consolidate()
                .inner
                .map(|((id, addr, name), _time, diff)| {

                    Update {
                        event: Event::Operate {
                            id,
                            addr,
                            name,
                        },
                        delta: diff,
                    }
                });

            let channels =
            replayed
                .flat_map(move |(ts,_,x)|
                    if let Channels(event) = x {
                        let ts = ((ts >> shift) + 1) << shift;
                        Some(((event.id, event.scope_addr, event.source, event.target), RootTimestamp::new(ts), 1 as isize))
                    }
                    else {
                        None
                    }
                )
                .as_collection()
                .consolidate()
                .inner
                .map(|((id, scope_addr, source, target), _time, diff)| {

                    let mut from_addr = scope_addr.clone();
                    from_addr.push(source.0);

                    let mut to_addr = scope_addr.clone();
                    to_addr.push(target.0);

                    Update {
                        event: Event::Channel {
                            id,
                            from_addr,
                            from_port: source.1,
                            to_addr,
                            to_port: target.1,
                        },
                        delta: diff,
                    }
                });

            let messages =
            replayed
                .flat_map(move |(ts,_,x)|
                    if let Messages(event) = x {
                        let ts = ((ts >> shift) + 1) << shift;
                        Some((event.channel, RootTimestamp::new(ts), event.length as isize))
                    }
                    else {
                        None
                    }
                )
                .as_collection()
                .count()
                .inner
                .map(|((channel_id, total_count), _time, diff)| {
                    Update {
                        event: Event::Messages {
                            channel_id,
                            total_count,
                        },
                        delta: diff,
                    }
                });

            let schedule =
            replayed
                .flat_map(move |(ts, setup, x)| if let Schedule(event) = x { Some((ts, setup.index, event)) } else { None })
                .unary(timely::dataflow::channels::pact::Pipeline, "Schedules", |_,_| {

                    let mut map = std::collections::HashMap::new();

                    move |input, output| {

                        input.for_each(|time, data| {
                            let mut session = output.session(&time);
                            for (ts, worker, event) in data.drain(..) {
                                let key = (worker, event.id);
                                match event.start_stop {
                                    timely::logging::StartStop::Start => {
                                        assert!(!map.contains_key(&key));
                                        map.insert(key, ts);
                                    },
                                    timely::logging::StartStop::Stop { activity: _work } => {
                                        assert!(map.contains_key(&key));
                                        let start = map.remove(&key).unwrap();
                                        // if work {
                                            let ts_clip = ((ts >> 25) + 1) << 25;
                                            session.give((key.1, RootTimestamp::new(ts_clip), (ts - start) as isize));
                                        // }
                                    }
                                }
                            }
                        });
                    }
                })
                .as_collection()
                .count()
                .inner
                .map(|((operate_id, elapsed_ns), _time, diff)| {
                    Update {
                        event: Event::Schedule {
                            operate_id,
                            elapsed_ns,
                        },
                        delta: diff,
                    }
                });

            operates
                .concat(&channels)
                .concat(&messages)
                .concat(&schedule)
                .sink(timely::dataflow::channels::pact::Pipeline, "ToVega", move |input| {

                    let mut updates = Vec::new();

                    input.for_each(|_time, dataz| {
                        for update in dataz.drain(..) {
                            updates.push(update);
                        }
                    });

                    updates.sort();
                    for i in 1 .. updates.len() {
                        if updates[i-1].event == updates[i].event {
                            updates[i].delta += updates[i-1].delta;
                            updates[i-1].delta = 0;
                        }
                    }
                    updates.retain(|x| x.delta != 0);

                    if !updates.is_empty() {
                        for chan in endpoint.lock().unwrap().iter_mut() {
                            let updates = Updates { updates: updates.clone() };
                            chan.send(updates).wait().unwrap();
                        }
                    }
                });
        });

    }).unwrap(); // asserts error-free execution

    }));

    fn index(_req: HttpRequest) -> Result<NamedFile> {
        let html_file = ::std::env::args().nth(2).expect("Must provide path to html file");
        Ok(NamedFile::open(html_file)?)
    }

    server::new(move || {
        let endpoint_actix = endpoint_actix.clone();
        App::new()
            .resource("/ws/", move |r| {
                let endpoint_actix = endpoint_actix.clone();
                r.f(move |req| ws::start(req, Ws { addr: endpoint_actix.clone() }))
            })
            .resource("/", |r| r.method(Method::GET).f(index))
    }).bind("0.0.0.0:9000").unwrap().run();
}