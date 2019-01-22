extern crate timely;
extern crate differential_dataflow;
extern crate actix;
extern crate actix_web;
extern crate futures;
extern crate timely_viz;

extern crate serde;
#[macro_use] extern crate serde_derive;
extern crate serde_json;

use std::time::Duration;
use std::sync::{Arc, Mutex};

use timely::dataflow::operators::{Map, capture::Replay};
use timely::logging::TimelyEvent::{Operates, Schedule};
use timely::dataflow::operators::Operator;

use differential_dataflow::AsCollection;
use differential_dataflow::operators::{Join, Count};

use actix::*;
use actix_web::*;
use actix_web::{App, HttpRequest, Result, http::Method, fs::NamedFile};

use futures::future::Future;

// ==== Web-server setup ====
use timely_viz::server::Ws;

#[derive(Serialize, Clone, Ord, PartialOrd, Eq, PartialEq)]
struct Update {
    data: String,   // name of data source.
    name: String,
    id: String,
    parent_id: String,
    size: isize,
    diff: isize,
}

#[derive(Serialize, Clone)]
struct Updates {
    updates: Vec<Update>,
}

impl actix::Message for Updates {
    type Result = ();
}

impl Handler<Updates> for Ws{
    type Result = ();

    fn handle(&mut self, msg: Updates, ctx: &mut Self::Context) {
        let text = serde_json::to_string(&msg).unwrap();
        println!("Sending: {}", text);
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
    let source_peers = args.next().unwrap().parse::<usize>().unwrap();
    let sockets = timely_viz::open_sockets(source_peers);

    timely::execute_from_args(std::env::args(), move |worker| {

        let endpoint = endpoint.clone();

        let sockets = sockets.clone();

        // create replayers from disjoint partition of source worker identifiers
        let replayers = timely_viz::make_replayers(sockets, worker.index(), worker.peers());

        worker.dataflow(|scope| {

            let replayed = replayers.replay_into(scope);

            let operates =
            replayed
                .flat_map(|(ts, _setup, datum)|
                    if let Operates(event) = datum {
                        let ts = Duration::from_secs(ts.as_secs() + 1);
                        Some((event, ts, 1))
                    }
                    else {
                        None
                    }
                )
                .as_collection();

            let schedule =
            replayed
                .flat_map(|(ts, worker, x)| if let Schedule(event) = x { Some((ts, worker, event)) } 
                    else { None })
                .unary(timely::dataflow::channels::pact::Pipeline, "Schedules", |_,_| {

                    let mut map = std::collections::HashMap::new();
                    let mut vec = Vec::new();
                    
                    move |input, output| {

                        input.for_each(|time, data| {
                            data.swap(&mut vec);
                            let mut session = output.session(&time);
                            for (ts, worker, event) in vec.drain(..) {
                                let key = (worker, event.id);
                                match event.start_stop {
                                    timely::logging::StartStop::Start => {
                                        assert!(!map.contains_key(&key));
                                        map.insert(key, ts);
                                    },
                                    timely::logging::StartStop::Stop => {
                                        assert!(map.contains_key(&key));
                                        let end = map.remove(&key).unwrap();
//                                        if work {
                                            let ts_clip = Duration::from_secs(ts.as_secs() + 1);
                                            let elapsed = ts - end;
                                            let elapsed_ns = (elapsed.as_secs() as isize) * 1_000_000_000 + (elapsed.subsec_nanos() as isize);
                                            session.give((key.1, ts_clip, elapsed_ns));
//                                        }
                                    }
                                }
                            }
                        });
                    }
                })
                .as_collection();

            operates
                .map(|x| (x.id, x.addr))
                .filter(|x| x.1[0] == 0)
                .semijoin(&schedule)
                .map(|(_id, addr)| addr)
                .count()
                .inner
                .sink(timely::dataflow::channels::pact::Pipeline, "ToVega", move |input| {
                    let mut updates = Updates { updates: Vec::new() };

                    input.for_each(|_time, dataz| {
                        for ((ref addr, count), _time, diff) in dataz.iter() {
                            if addr.len() > 0 {
                                updates.updates.push(
                                    Update {
                                        data: "tree".to_string(),
                                        name: format!("{:?}", &addr[..]),
                                        id: format!("{:?}", &addr[..]),
                                        parent_id: format!("{:?}", &addr[.. addr.len()-1]),
                                        size: *count,
                                        diff: *diff,
                                    }
                                );
                            }
                        }
                    });

                    updates.updates.sort();
                    for i in 1 .. updates.updates.len() {
                        if updates.updates[i-1].id == updates.updates[i].id && updates.updates[i-1].size == updates.updates[i].size {
                            updates.updates[i].diff += updates.updates[i-1].diff;
                            updates.updates[i-1].diff = 0;
                        }
                    }
                    updates.updates.retain(|x| x.diff != 0);
                    if !updates.updates.is_empty() {
                        for chan in endpoint.lock().unwrap().iter_mut() {
                            chan.send(updates.clone()).wait().unwrap();
                        }
                    }
                });
        });

    }).unwrap(); // asserts error-free execution
    }));

    fn index(_req: HttpRequest) -> Result<NamedFile> {
        Ok(NamedFile::open("html/schedule.html")?)
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
