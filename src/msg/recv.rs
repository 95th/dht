use crate::id::NodeId;
use crate::msg::TxnId;
use ben::decode::{Dict, List};
use ben::{Decode, Decoder};
use std::convert::TryInto;

#[derive(Debug)]
pub struct Query<'a, 'p> {
    pub txn_id: TxnId,
    pub args: Dict<'a, 'p>,
    pub id: &'a NodeId,
    pub kind: QueryKind,
}

#[derive(Debug)]
pub struct Response<'a, 'p> {
    pub txn_id: TxnId,
    pub body: Dict<'a, 'p>,
    pub id: &'a NodeId,
}

#[derive(Debug)]
pub struct ErrorResponse<'a, 'p> {
    pub txn_id: TxnId,
    pub list: List<'a, 'p>,
}

#[derive(Debug)]
pub enum Msg<'a, 'p> {
    Query(Query<'a, 'p>),
    Response(Response<'a, 'p>),
    Error(ErrorResponse<'a, 'p>),
}

#[derive(Debug, Clone, Copy, PartialOrd, PartialEq)]
pub enum QueryKind {
    Ping,
    FindNode,
    GetPeers,
    AnnouncePeer,
}

macro_rules! check {
    ($expr:expr, $err:literal) => {
        match $expr {
            Some(val) => val,
            None => return Err(ben::Error::Other($err)),
        }
    };
}

impl<'a, 'p> Decode<'a, 'p> for Msg<'a, 'p> {
    fn decode(decoder: Decoder<'a, 'p>) -> ben::Result<Self> {
        use ben::Error::Other;

        let dict = check!(decoder.into_dict(), "Not a dict");
        let y = check!(dict.get_bytes("y"), "Message type is required");
        let txn_id = check!(dict.get_bytes("t"), "Transaction ID is required");
        let txn_id = check!(txn_id.try_into().ok(), "Transaction ID must be 2 bytes");
        let txn_id = TxnId(u16::from_be_bytes(txn_id));

        let msg = match y {
            b"q" => {
                let q = check!(dict.get_bytes("q"), "Query type is required");
                let query_kind = match q {
                    b"ping" => QueryKind::Ping,
                    b"find_node" => QueryKind::FindNode,
                    b"get_peers" => QueryKind::GetPeers,
                    b"announce_peer" => QueryKind::AnnouncePeer,
                    other => {
                        log::trace!("Unexpected Query type: {:?}", other);
                        return Err(Other("Unexpected Query type"));
                    }
                };
                let args = check!(dict.get_dict("a"), "Query args are required");
                Msg::Query(Query {
                    kind: query_kind,
                    id: extract_id(&args)?,
                    txn_id,
                    args,
                })
            }
            b"r" => {
                let body = check!(dict.get_dict("r"), "Response args are required");
                Msg::Response(Response {
                    id: extract_id(&body)?,
                    txn_id,
                    body,
                })
            }
            b"e" => {
                let list = check!(dict.get_list("r"), "Error list is required");
                Msg::Error(ErrorResponse { txn_id, list })
            }
            other => {
                log::trace!("Unexpected Message type: {:?}", other);
                return Err(Other("Unexpected Message type"));
            }
        };

        Ok(msg)
    }
}

fn extract_id<'a>(dict: &Dict<'a, '_>) -> ben::Result<&'a NodeId> {
    let id = check!(dict.get_bytes("id"), "ID is required");
    if id.len() == 20 {
        let ptr = id.as_ptr() as *const NodeId;
        unsafe { Ok(&*ptr) }
    } else {
        Err(ben::Error::Other("ID must be 20 bytes long"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ben::Parser;

    #[test]
    fn incoming_ping() {
        let expected: &[u8] = b"d1:ad2:id20:\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01e1:q4:ping1:t2:\x00\n1:y1:qe";
        let mut parser = Parser::new();
        let msg = parser.parse::<Msg>(expected).unwrap();

        match msg {
            Msg::Query(query) => {
                assert_eq!(query.kind, QueryKind::Ping);
                assert_eq!(query.id, &NodeId::all(1));
                assert_eq!(query.txn_id, TxnId(10));
                assert_eq!(b"d2:id20:\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01e", query.args.as_raw_bytes());
            }
            _ => {
                panic!("Incorrect msg type");
            }
        }
    }
}
