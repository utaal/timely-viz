extern crate timely;
extern crate differential_dataflow;
extern crate timely_viz;

extern crate abomonation;

use std::time::Duration;

use timely::dataflow::operators::{Map, capture::Replay};
use timely::logging::TimelyEvent::{Operates, Channels};

use differential_dataflow::collection::AsCollection;

fn main() {
    let mut args = ::std::env::args();
    args.next().unwrap();

    // the number of workers in the computation we're examining
    let source_peers = args.next().unwrap().parse::<usize>().unwrap();
    // one socket per worker in the computation we're examining
    let sockets = timely_viz::open_sockets(source_peers);

    timely::execute_from_args(args, move |worker| {
        let sockets = sockets.clone();

        let replayers = timely_viz::make_replayers(sockets, worker.index(), worker.peers());

        worker.dataflow::<Duration,_,_>(|scope| {
            let stream =
            replayers
                .replay_into(scope);

            let _operates =
            stream
                .flat_map(|(t,_,x)| {
                    if let Operates(event) = x {
                        Some((event, t, 1 as isize))
                    } else {
                        None
                    }
                })
                .as_collection()
                // only keep elements that came from worker 0
                // (the first element of "addr" is the worker id)
                .filter(|x| *x.addr.first().unwrap() == 0)
                .inspect(|x| println!("Operates: {:?}", x));

            let _channels =
            stream
                .flat_map(|(t,_,x)| {
                    if let Channels(event) = x {
                        Some((event, t, 1 as isize))
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
