use verdandi_node::Msg;

pub type Error = postcard::Error;

pub fn encode(msg: &Msg) -> Vec<u8> {
    postcard::to_allocvec(msg).expect("a Msg always encodes to a Vec")
}

pub fn decode(bytes: &[u8]) -> Result<Msg, Error> {
    postcard::from_bytes(bytes)
}

mod link;
pub use link::{Link, MAX_FRAME, key_ok};
mod net;
pub use net::{Net, Wire};
mod hello;
pub use hello::{
    ClusterId, HELLO_MAX, Hello, MEMBERS_MAX, Members, Reply, open, open_members, seal,
    seal_members,
};
