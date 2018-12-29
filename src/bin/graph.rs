extern crate timely;
extern crate differential_dataflow;
extern crate timely_viz;

extern crate abomonation;
#[macro_use] extern crate abomonation_derive;

extern crate actix;
extern crate actix_web;
extern crate futures;

extern crate serde;
#[macro_use] extern crate serde_derive;
extern crate serde_json;

use std::sync::{Arc, Mutex};

use timely::dataflow::operators::{Map, capture::Replay, Concat, Inspect};
use timely::logging::TimelyEvent::{Operates, Channels};

use differential_dataflow::collection::AsCollection;
use differential_dataflow::operators::{Consolidate, Join, Threshold};

use actix::*;
use actix_web::*;
use actix_web::{App, HttpRequest, Result, http::Method, fs::NamedFile};

use futures::future::Future;

#[derive(Serialize, Abomonation, Clone, PartialOrd, PartialEq, Eq, Ord, Debug)]
enum Update {
    Operate {
        addr: Vec<usize>,
        name: String,
    },
    Channel {
        id: usize,
        from_addr: Vec<usize>,
        to_addr: Vec<usize>,
        subgraph: bool,
    },
}

// ==== Web-server setup ====
use timely_viz::server::Ws;

#[derive(Serialize, Clone)]
struct Updates {
    updates: Vec<Update>,
}

impl actix::Message for Update {
    type Result = ();
}

impl actix::Message for Updates {
    type Result = ();
}

impl Handler<Updates> for Ws {
    type Result = ();

    fn handle(&mut self, msg: Updates, ctx: &mut Self::Context) {
        ctx.text(serde_json::to_string(&msg).unwrap());
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
    // one socket per worker in the computation we're examining
    let sockets = timely_viz::open_sockets(source_peers);

    timely::execute_from_args(args, move |worker| {
        let endpoint = endpoint.clone();
        let sockets = sockets.clone();

        // create replayers from disjoint partition of source worker identifiers.
        let replayers = timely_viz::make_replayers(sockets, worker.index(), worker.peers());

        worker.dataflow(|scope| {
            let stream =
            replayers
                .replay_into(scope);

            let operates =
            stream
                .flat_map(|(t,_,x)| if let Operates(event) = x { Some((event, t, 1 as isize)) } else { None })
                .as_collection();

            let channels =
            stream
                .flat_map(|(t,_,x)| if let Channels(event) = x { Some((event, t, 1 as isize)) } else { None })
                .as_collection();

            // == Fix addresses so we can connect operators outside and inside subgraphs ==

            let operates = operates.map(|event| (event.addr, event.name)).inspect(|x| println!("Operates: {:?}", x.0));

            let operates_anti = operates.map(|(mut addr, _)| {
                addr.pop();
                addr
            });

            let operates_without_subg = operates.antijoin(&operates_anti.distinct());

            let channels = channels.map(
                |event| (event.id, (event.scope_addr, event.source, event.target))).inspect(|x| println!("Channels: {:?}", x.0));

            ({
                operates_without_subg
                    .filter(|(addr, _)| addr[0] == 0)
                    .consolidate()
                    .inner
                    .map(move |((addr, name), _, _)| {
                        let mut addr = addr.clone();
                        addr.remove(0);
                        Update::Operate {
                            addr: addr.clone(),
                            name: name.clone(),
                        }
                    })
            }).concat(&{
                let worker_0 = channels
                    .filter(|(_, (scope_addr, _, _))| scope_addr[0] == 0)
                    .map(|(id, (scope_addr, from, to))| {
                        let mut scope_addr = scope_addr.clone();
                        scope_addr.remove(0);
                        (id, (scope_addr, from, to))
                    });

                let subg_edges = worker_0
                    .filter(|(_, (_, from, to))| from.0 == 0 || to.0 == 0)
                    .flat_map(|(id, (scope_addr, from, to))| vec![
                              ((scope_addr.clone(), from.1), (id, (scope_addr.clone(), from, to))),
                              ((scope_addr.clone(), to.1), (id, (scope_addr.clone(), from, to)))].into_iter());

                let subg_incoming = subg_edges
                    .join_map(&worker_0.map(|(id, (scope_addr, from, to))| {
                        let mut from_addr = scope_addr.clone();
                        from_addr.push(from.0);
                        ((from_addr, from.1), (id, (scope_addr, from, to)))
                    }), |(_from_addr, _), (id, (scope_addr_from, from, _)), (_, (scope_addr_to, _, to))| {
                        let mut from_addr = scope_addr_from.clone();
                        from_addr.push(from.0);
                        let mut to_addr = scope_addr_to.clone();
                        to_addr.push(to.0);
                        Update::Channel {
                            id: *id,
                            subgraph: true,
                            from_addr,
                            to_addr,
                        }
                    });

                let subg_outgoing = subg_edges
                    .join_map(&worker_0.map(|(id, (scope_addr, from, to))| {
                        let mut to_addr = scope_addr.clone();
                        to_addr.push(to.0);
                        ((to_addr, to.1), (id, (scope_addr, from, to)))
                    }), |(_to_addr, _), (id, (scope_addr_to, _, to)), (_, (scope_addr_from, from, _))| {
                        let mut from_addr = scope_addr_from.clone();
                        from_addr.push(from.0);
                        let mut to_addr = scope_addr_to.clone();
                        to_addr.push(to.0);
                        Update::Channel {
                            id: *id,
                            subgraph: true,
                            from_addr,
                            to_addr,
                        }
                    });

                worker_0
                    .filter(|(_, (_, from, to))| from.0 != 0 && to.0 != 0)
                    .consolidate()
                    .inner
                    .map(move |((id, (scope_addr, (from, _), (to, _))), _, _)| {
                        let mut from_addr = scope_addr.clone();
                        from_addr.push(from);
                        let mut to_addr = scope_addr.clone();
                        to_addr.push(to);
                        Update::Channel {
                            id,
                            subgraph: false,
                            from_addr,
                            to_addr,
                        }
                    })
                    .concat(&subg_incoming.inner.map(|(x, _, _)| x))
                    .concat(&subg_outgoing.inner.map(|(x, _, _)| x))
            // Push the updates to the browser
            }).inspect_batch(move |_, x| {
                for chan in endpoint.lock().unwrap().iter_mut() {
                    chan.send(Updates { updates: x.to_vec() }).wait().unwrap();
                }
            });
        })
    }).unwrap(); // asserts error-free execution

    }));


    // == Start the web-server ==
    fn index(_req: HttpRequest) -> Result<NamedFile> {
        Ok(NamedFile::open("html/graph.html")?)
    }

    server::new(move || {
        let endpoint_actix = endpoint_actix.clone();
        App::new()
            .resource("/ws/", move |r| {
                let endpoint_actix = endpoint_actix.clone();
                r.f(move |req| ws::start(req, Ws { addr: endpoint_actix.clone() }))
            })
            .resource("/", |r| r.method(Method::GET).f(index))
    }).bind("0.0.0.0:9000").unwrap().disable_signals().run();
}
