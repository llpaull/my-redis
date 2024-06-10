use bytes::{Buf, Bytes};
use std::io::Cursor;

const STRING: u8 = b'+';
const ERROR: u8 = b'-';
const INTEGER: u8 = b':';
const BULK: u8 = b'$';
const ARRAY: u8 = b'*';

type ResultOpt<T> = std::result::Result<Option<T>, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Debug, PartialEq, Eq)]
pub enum RESPType {
    String(String),
    Error(String),
    Integer(i64),
    Bulk(Bytes),
    Array(Vec<RESPType>),
    Null,
}

pub struct RESPParser {}

// public functions
impl RESPParser {
    pub fn parse(src: &mut Cursor<&[u8]>) -> ResultOpt<RESPType> {
        if let Some(_) = src.get_ref().windows(2).find(|window| window == b"\r\n") {
            match Self::get_u8(src) {
                None => Ok(None),
                Some(char) => match char {
                    STRING => {
                        Self::to_result(Self::parse_simple(src)?, |x: String| RESPType::String(x))
                    }
                    ERROR => {
                        Self::to_result(Self::parse_simple(src)?, |x: String| RESPType::Error(x))
                    }
                    INTEGER => {
                        Self::to_result(Self::parse_integer(src)?, |x: i64| RESPType::Integer(x))
                    }
                    BULK => Self::to_result(Self::parse_bulk(src)?, |x: Option<Bytes>| match x {
                        None => RESPType::Null,
                        Some(val) => RESPType::Bulk(val),
                    }),
                    ARRAY => {
                        Self::to_result(Self::parse_array(src)?, |x: Option<Vec<RESPType>>| match x
                        {
                            None => RESPType::Null,
                            Some(val) => RESPType::Array(val),
                        })
                    }
                    _ => todo!(),
                },
            }
        } else {
            // no CRLF so no full message
            Ok(None)
        }
    }
}

// private helper functions
impl RESPParser {
    fn get_u8(src: &mut Cursor<&[u8]>) -> Option<u8> {
        if src.has_remaining() {
            return Some(src.get_u8());
        }

        None
    }

    fn to_result<T, F>(option: Option<T>, constructor: F) -> ResultOpt<RESPType>
    where
        F: FnOnce(T) -> RESPType,
    {
        match option {
            None => Ok(None),
            Some(val) => Ok(Some(constructor(val))),
        }
    }
}

// private parsing functions
impl RESPParser {
    fn parse_simple(src: &mut Cursor<&[u8]>) -> ResultOpt<String> {
        let start = src.position() as usize;
        let size = match src.get_ref()[start..]
            .windows(2)
            .position(|window| window == b"\r\n")
        {
            // incomplete frame
            None => return Ok(None),
            Some(val) => val,
        };
        let mut result = String::new();

        for _ in 0..size {
            match Self::get_u8(src).unwrap() {
                // start - 1 because of type identifier
                b'\r' => return Err("CR not allowed in simples".into()),
                b'\n' => return Err("LF not allowed in simples".into()),
                c => result.push(c.into()),
            }
        }
        src.advance(2);
        // simple {string, error} type
        Ok(Some(result))
    }

    fn parse_integer(src: &mut Cursor<&[u8]>) -> ResultOpt<i64> {
        let start = src.position() as usize;
        let size = match src.get_ref()[start..]
            .windows(2)
            .position(|window| window == b"\r\n")
        {
            // incomplete frame
            None => return Ok(None),
            Some(val) => val,
        };
        let mut result: i64 = 0;

        let sign: i64 = match Self::get_u8(src).unwrap() {
            b'-' => -1,
            b'+' => 1,
            d @ (b'0'..=b'9') => {
                result += (d - b'0') as i64;
                1
            }
            _ => return Err("No integer found after integer type declaration".into()),
        };

        for _ in 0..size - 1 {
            match Self::get_u8(src).unwrap() {
                b'\r' => return Err("CR not allowed in integers".into()),
                b'\n' => return Err("LF not allowed in integers".into()),
                d @ (b'0'..=b'9') => result = (result * 10) + (d - b'0') as i64,
                _ => return Err("Digits are the only thing allowed in integers".into()),
            }
        }

        src.advance(2);
        // integer type
        Ok(Some(result * sign))
    }

    fn parse_bulk(src: &mut Cursor<&[u8]>) -> ResultOpt<Option<Bytes>> {
        let start = src.position() as usize;
        // check if incomplete frame
        let size_int = match src.get_ref()[start..]
            .windows(2)
            .position(|window| window == b"\r\n")
        {
            None => return Ok(None),
            Some(val) => val,
        };

        // check if null type
        if src.get_ref()[start..start + 2] == *b"-1" {
            src.advance(4);
            return Ok(Some(None));
        }

        // get len of bulk string
        let len = str::parse::<usize>(&String::from_utf8(
            src.get_ref()[start..(start + size_int)].to_vec(),
        )?)?;

        src.advance(size_int + 2);

        let mut result = String::new();

        if len == 0 {
            return Ok(Some(Some(Bytes::from(""))));
        }

        for _ in 0..len {
            if src.has_remaining() {
                result.push(src.get_u8().into());
            } else {
                return Ok(None);
            }
        }

        match (Self::get_u8(src), Self::get_u8(src)) {
            (Some(b'\r'), Some(b'\n')) => {}
            (None, _) => return Ok(None),
            (_, None) => return Ok(None),
            _ => {
                return Err(
                    "Unexpected symbols at end of bulk, either wrong length or misformed string"
                        .into(),
                )
            }
        }

        // bulk type
        Ok(Some(Some(result.into())))
    }

    fn parse_array(src: &mut Cursor<&[u8]>) -> ResultOpt<Option<Vec<RESPType>>> {
        let start = src.position() as usize;
        // check if incomplete frame
        let size_int = match src.get_ref()[start..]
            .windows(2)
            .position(|window| window == b"\r\n")
        {
            None => return Ok(None),
            Some(val) => val,
        };

        // check if null type
        if src.get_ref()[start..start + 2] == *b"-1" {
            src.advance(4);
            return Ok(Some(None));
        }

        // get len of bulk string
        let len = str::parse::<usize>(&String::from_utf8(
            src.get_ref()[start..(start + size_int)].to_vec(),
        )?)?;

        src.advance(size_int + 2);
        let mut result = vec![];

        for _ in 0..len {
            match Self::parse(src)? {
                None => return Ok(None),
                Some(val) => result.push(val),
            }
        }

        // array type
        Ok(Some(Some(result)))
    }
}

pub struct RESPSerializer {}

// public functions
impl RESPSerializer {
    pub fn serialize(msg: &RESPType) -> crate::Result<Bytes> {
        match msg {
            RESPType::String(str) => Ok(Bytes::from(format!("+{}", Self::serialize_simple(str)?))),
            RESPType::Error(str) => Ok(Bytes::from(format!("-{}", Self::serialize_simple(str)?))),
            RESPType::Integer(n) => Ok(Bytes::from(format!(":{}\r\n", n))),
            RESPType::Bulk(str) => Ok(Self::serialize_bulk(str)),
            RESPType::Array(arr) => Ok(Self::serialize_array(arr)?),
            RESPType::Null => Ok(Bytes::from("$-1\r\n")),
        }
    }
}

// private serialization functions
impl RESPSerializer {
    fn serialize_simple(src: &String) -> crate::Result<String> {
        if src
            .bytes()
            .find(|char| *char == b'\r' || *char == b'\n')
            .is_some()
        {
            return Err("CR or LF is not allowed in strings and errors".into());
        } else {
            return Ok(String::from(src.to_owned() + "\r\n"));
        }
    }

    fn serialize_bulk(src: &Bytes) -> Bytes {
        let len = src.len();
        match len {
            0 => Bytes::from("$0\r\n"),
            _ => Bytes::from(format!(
                "${}\r\n{}\r\n",
                len,
                String::from_utf8(src[..].to_vec()).unwrap()
            )),
        }
    }

    fn serialize_array(src: &Vec<RESPType>) -> crate::Result<Bytes> {
        let mut result = String::new();
        result.push_str(&format!("*{}\r\n", src.len()));
        for resp in src {
            result.extend(String::from_utf8(Self::serialize(resp)?.to_vec()));
        }
        Ok(Bytes::from(result))
    }
}

// unit tests
#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    fn parse(src: &str) -> ResultOpt<RESPType> {
        let buf = BytesMut::from(src);
        RESPParser::parse(&mut Cursor::new(&buf[..]))
    }

    #[test]
    fn test_parse_string() {
        assert!(matches!(
            parse("+hello world\r\n"),
            Ok(Some(RESPType::String(ref s))) if s == "hello world"
        ));

        assert!(matches!(parse("+hello\rworld\r\n"), Err(_),));
        assert!(matches!(parse("+hello\nworld\r\n"), Err(_),));
        assert!(matches!(parse("+hello world\r"), Ok(None)));
    }

    #[test]
    fn test_parse_error() {
        assert!(matches!(
            parse("-ERR incorrect type\r\n"),
            Ok(Some(RESPType::Error(ref s))) if s == "ERR incorrect type"
        ));

        assert!(matches!(parse("-ERR\rincorrect type\r\n"), Err(_),));
        assert!(matches!(parse("-ERR\nincorrect type\r\n"), Err(_),));
        assert!(matches!(parse("-ERR incorrect type\r"), Ok(None)));
    }

    #[test]
    fn test_parse_integer() {
        assert!(matches!(
            parse(":123\r\n"),
            Ok(Some(RESPType::Integer(123)))
        ));
        assert!(matches!(
            parse(":+123\r\n"),
            Ok(Some(RESPType::Integer(123)))
        ));
        assert!(matches!(
            parse(":-123\r\n"),
            Ok(Some(RESPType::Integer(-123)))
        ));
        assert!(matches!(parse(":1a23\r\n"), Err(_)));
    }

    #[test]
    fn test_parse_bulk() {
        assert!(matches!(parse("$-1\r\n"), Ok(Some(RESPType::Null))));
        assert!(matches!(
            parse("$3\r\nget\r\n"),
            Ok(Some(RESPType::Bulk(ref s))) if **s == *b"get"
        ));
        assert!(matches!(
            parse("$0\r\n\r\n"),
            Ok(Some(RESPType::Bulk(ref s))) if **s == *b""
        ));
        assert!(matches!(
            parse("$11\r\nhello\nworld\r\n"),
            Ok(Some(RESPType::Bulk(ref s))) if **s == *b"hello\nworld"
        ));
        assert!(matches!(
            parse("$11\r\nhello\rworld\r\n"),
            Ok(Some(RESPType::Bulk(ref s))) if **s == *b"hello\rworld"
        ));
        assert!(matches!(
            parse("$12\r\nhello\r\nworld\r\n"),
            Ok(Some(RESPType::Bulk(ref s))) if **s == *b"hello\r\nworld"
        ));
        assert!(matches!(
            parse("$0\r\n"),
            Ok(Some(RESPType::Bulk(ref s))) if **s == *b""
        ));
    }

    #[test]
    fn test_parse_array() {
        assert!(matches!(
           parse("*1\r\n$4\r\nping\r\n"),
           Ok(Some(RESPType::Array(ref vec))) if vec.eq(&vec![RESPType::Bulk(Bytes::from("ping"))])
        ));
        assert!(matches!(
           parse("*2\r\n$4\r\necho\r\n$11\r\nhello world\r\n"),
           Ok(Some(RESPType::Array(ref vec))) if vec.eq(&vec![RESPType::Bulk(Bytes::from("echo")), RESPType::Bulk(Bytes::from("hello world"))])
        ));
        assert!(matches!(
           parse("*2\r\n$3\r\nget\r\n$3\r\nkey\r\n"),
           Ok(Some(RESPType::Array(ref vec))) if vec.eq(&vec![RESPType::Bulk(Bytes::from("get")), RESPType::Bulk(Bytes::from("key"))])
        ));
        assert!(matches!(parse("*-1\r\n"), Ok(Some(RESPType::Null))));
        assert!(matches!(parse("*2\r\n$3\r\nget\r\n$3\r\nkey"), Ok(None)));
        assert!(matches!(parse("*2\r\n$3\r\nget\r\n$3\r\nkey\r"), Ok(None)));
    }

    #[test]
    fn test_serialize_string() {
        assert!(matches!(
            RESPSerializer::serialize(&RESPType::String(String::from("hello world"))),
            Ok(ref b) if *b == Bytes::from("+hello world\r\n")
        ));
        assert!(matches!(
            RESPSerializer::serialize(&RESPType::String(String::from("hello\rworld"))),
            Err(_)
        ));
        assert!(matches!(
            RESPSerializer::serialize(&RESPType::String(String::from("hello\nworld"))),
            Err(_)
        ));
        assert!(matches!(
            RESPSerializer::serialize(&RESPType::String(String::from("hello\r\nworld"))),
            Err(_)
        ));
    }

    #[test]
    fn test_serialize_error() {
        assert!(matches!(
            RESPSerializer::serialize(&RESPType::Error(String::from("ERR something wrong"))),
            Ok(ref b) if *b == Bytes::from("-ERR something wrong\r\n")
        ));
        assert!(matches!(
            RESPSerializer::serialize(&RESPType::Error(String::from("ERR\rsomething wrong"))),
            Err(_)
        ));
        assert!(matches!(
            RESPSerializer::serialize(&RESPType::Error(String::from("ERR\nsomething wrong"))),
            Err(_)
        ));
        assert!(matches!(
            RESPSerializer::serialize(&RESPType::String(String::from("ERR\r\nsomething wrong"))),
            Err(_)
        ));
    }

    #[test]
    fn test_serialize_integer() {
        assert!(matches!(
            RESPSerializer::serialize(&RESPType::Integer(123)),
            Ok(ref b) if *b == Bytes::from(":123\r\n")
        ));
        assert!(matches!(
            RESPSerializer::serialize(&RESPType::Integer(-123)),
            Ok(ref b) if *b == Bytes::from(":-123\r\n")
        ));
    }

    #[test]
    fn test_serialize_bulk() {
        assert!(matches!(
            RESPSerializer::serialize(&RESPType::Bulk(Bytes::from("this is a bulk message"))),
            Ok(ref b) if *b == Bytes::from("$22\r\nthis is a bulk message\r\n")
        ));
        assert!(matches!(
            RESPSerializer::serialize(&RESPType::Bulk(Bytes::from("this is a bulk message\r with a CR"))),
            Ok(ref b) if *b == Bytes::from("$33\r\nthis is a bulk message\r with a CR\r\n")
        ));
        assert!(matches!(
            RESPSerializer::serialize(&RESPType::Bulk(Bytes::from("this is a bulk message\n with a LF"))),
            Ok(ref b) if *b == Bytes::from("$33\r\nthis is a bulk message\n with a LF\r\n")
        ));
        assert!(matches!(
            RESPSerializer::serialize(&RESPType::Bulk(Bytes::from("this is a bulk message\r\n with a CRLF"))),
            Ok(ref b) if *b == Bytes::from("$36\r\nthis is a bulk message\r\n with a CRLF\r\n")
        ));
    }

    #[test]
    fn test_serialize_array() {
        assert!(matches!(RESPSerializer::serialize(
            &RESPType::Array(
                vec![
                    RESPType::String(String::from("hello world")),
                    RESPType::Error(String::from("ERR something wrong")),
                    RESPType::Integer(-123),
                    RESPType::Bulk(Bytes::from("this is a bulk message\r\n with a CRLF"))
                ]
            )),
            Ok(ref b) if *b == Bytes::from("*4\r\n+hello world\r\n-ERR something wrong\r\n:-123\r\n$36\r\nthis is a bulk message\r\n with a CRLF\r\n")
        ));
        assert!(matches!(
            RESPSerializer::serialize(&RESPType::Array(vec![
                RESPType::String(String::from("hello\rworld")),
                RESPType::Error(String::from("ERR something wrong")),
                RESPType::Integer(-123),
                RESPType::Bulk(Bytes::from("this is a bulk message\r\n with a CRLF"))
            ])),
            Err(_)
        ));
        assert!(matches!(
            RESPSerializer::serialize(&RESPType::Array(vec![
                RESPType::String(String::from("hello world")),
                RESPType::Error(String::from("ERR\nsomething wrong")),
                RESPType::Integer(-123),
                RESPType::Bulk(Bytes::from("this is a bulk message\r\n with a CRLF"))
            ])),
            Err(_)
        ));
    }

    #[test]
    fn test_serialize_null() {
        assert!(matches!(
            RESPSerializer::serialize(&RESPType::Null),
            Ok(ref b) if *b == Bytes::from("$-1\r\n")
        ));
    }
}
