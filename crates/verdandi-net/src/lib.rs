use verdandi_node::Msg;

pub type Error = postcard::Error;

pub fn encode(msg: &Msg) -> Vec<u8> {
    postcard::to_allocvec(msg).expect("a Msg always encodes to a Vec")
}

pub fn decode(bytes: &[u8]) -> Result<Msg, Error> {
    postcard::from_bytes(bytes)
}
