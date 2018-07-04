extern crate timely;
extern crate differential_dataflow;

extern crate actix;
extern crate actix_web;

pub mod server;

use std::sync::{Arc, Mutex};
use std::net::{TcpStream, TcpListener};

use timely::dataflow::operators::capture::EventReader;
use timely::progress::nested::product::Product;
use timely::progress::timestamp::RootTimestamp;
use timely::logging::{TimelySetup, TimelyEvent};

/// Listens on 127.0.0.1:8000 and opens `source_peers` sockets from the
/// computations we're examining.
pub fn open_sockets(source_peers: usize) -> Arc<Mutex<Vec<Option<TcpStream>>>> {
    let listener = TcpListener::bind("127.0.0.1:8000").unwrap();
    Arc::new(Mutex::new(
            (0..source_peers).map(|_| {
                Some(listener.incoming().next().unwrap().unwrap())
            }).collect::<Vec<_>>()))
}

/// Construct replayers that read data from sockets and can stream it into
/// timely dataflow.
pub fn make_replayers(sockets: Arc<Mutex<Vec<Option<TcpStream>>>>, index: usize, peers: usize) -> Vec<EventReader<Product<RootTimestamp, u64>, (u64, TimelySetup, TimelyEvent), TcpStream>> {

    sockets.lock().unwrap()
        .iter_mut().enumerate()
        .filter(|(i, _)| *i % peers == index)
        .map(move |(_, s)| s.take().unwrap())
        .map(|r| EventReader::<Product<RootTimestamp, u64>, (u64, TimelySetup, TimelyEvent),_>::new(r))
        .collect::<Vec<_>>()
}
