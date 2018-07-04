/// Web-server

use actix::*;
use actix_web::*;

use std::sync::{Arc, Mutex};

pub struct Ws {
    pub addr: Arc<Mutex<Vec<Addr<Syn, Ws>>>>,
}

impl Actor for Ws {
    type Context = ws::WebsocketContext<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        let addr: Addr<Syn, Ws> = ctx.address();
        self.addr.lock().unwrap().push(addr);
    }
}

impl StreamHandler<ws::Message, ws::ProtocolError> for Ws {
    fn handle(&mut self, msg: ws::Message, ctx: &mut Self::Context) {
        match msg {
            ws::Message::Ping(msg) => ctx.pong(&msg),
            _ => (),
        }
    }
}
