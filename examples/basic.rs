extern crate timely;
extern crate differential_dataflow;

extern crate abomonation;
#[macro_use] extern crate abomonation_derive;

use std::sync::{Arc, Mutex};
use std::net::{TcpStream, TcpListener};

use timely::dataflow::operators::{Map, capture::{EventReader, Replay}, Concat, Inspect};
use timely::progress::nested::product::Product;
use timely::progress::timestamp::RootTimestamp;
use timely::logging::{TimelySetup, TimelyEvent};
use timely::logging::TimelyEvent::{Operates, Channels, Messages};

use differential_dataflow::collection::AsCollection;
use differential_dataflow::operators::{Consolidate, Join, Threshold};

/// Listens on 127.0.0.1:8000 and opens `source_peers` sockets from the
/// computations we're examining.
fn open_sockets(source_peers: usize) -> Arc<Mutex<Vec<Option<TcpStream>>>> {
    let listener = TcpListener::bind("127.0.0.1:8000").unwrap();
    Arc::new(Mutex::new(
            (0..source_peers).map(|_| {
                Some(listener.incoming().next().unwrap().unwrap())
            }).collect::<Vec<_>>()))
}

/// Construct replayers that read data from sockets and can stream it into
/// timely dataflow.
fn make_replayers(sockets: Arc<Mutex<Vec<Option<TcpStream>>>>, index: usize, peers: usize) -> Vec<EventReader<Product<RootTimestamp, u64>, (u64, TimelySetup, TimelyEvent), TcpStream>> {

    sockets.lock().unwrap()
        .iter_mut().enumerate()
        .filter(|(i, _)| *i % peers == index)
        .map(move |(_, s)| s.take().unwrap())
        .map(|r| EventReader::<Product<RootTimestamp, u64>, (u64, TimelySetup, TimelyEvent),_>::new(r))
        .collect::<Vec<_>>()
}

fn main() {
    let mut args = ::std::env::args();
    args.next().unwrap();

    // the number of workers in the computation we're examining
    let source_peers = args.next().unwrap().parse::<usize>().unwrap();
    // one socket per worker in the computation we're examining
    let sockets = open_sockets(source_peers);

    timely::execute_from_args(args, move |worker| {
        let mut sockets = sockets.clone();

        let replayers = make_replayers(sockets, worker.index(), worker.peers());

        worker.dataflow(|scope| {
            let stream =
            replayers
                .replay_into(scope);

            let operates =
            stream
                .flat_map(|(t,_,x)| {
                    if let Operates(event) = x {
                        Some((event, RootTimestamp::new(t), 1 as isize))
                    } else {
                        None
                    }
                })
                .as_collection()
                // only keep elements that came from worker 0
                // (the first element of "addr" is the worker id)
                .filter(|x| *x.addr.first().unwrap() == 0)
                .inspect(|x| println!("Operates: {:?}", x));

            let channels =
            stream
                .flat_map(|(t,_,x)| {
                    if let Channels(event) = x {
                        Some((event, RootTimestamp::new(t), 1 as isize))
                    } else {
                        None
                    }
                })
                .as_collection()
                .filter(|x| *x.scope_addr.first().unwrap() == 0)
                .inspect(|x| println!("Channels: {:?}", x));
        });
    }).unwrap();
}
