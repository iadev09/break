use bytes::Bytes;
use http::StatusCode;

use super::Outcome;
use crate::BreakReason;

#[derive(Clone, Copy)]
enum State {
    Head,
    Length(u64),
    ChunkSize,
    Chunk(u64),
    ChunkEnd(u8),
    Trailers,
    UntilClose,
}

pub(super) struct Reply {
    pub outcome: Outcome,
    pub close: bool,
}

/// Incremental framing keeps cancellation of socket-read readiness lossless.
/// Only headers/size lines are buffered; payload bytes are checked in place.
pub(super) struct Decoder {
    state: State,
    line: Vec<u8>,
    expected_status: Option<StatusCode>,
    expected_body: Option<Bytes>,
    offset: usize,
    matches: bool,
    status: u16,
    close: bool,
}

impl Decoder {
    pub fn new(
        status: Option<StatusCode>,
        body: Option<Bytes>,
    ) -> Self {
        Self {
            state: State::Head,
            line: Vec::new(),
            expected_status: status,
            expected_body: body,
            offset: 0,
            matches: true,
            status: 0,
            close: false,
        }
    }

    pub fn feed(
        &mut self,
        input: &[u8],
    ) -> Result<(usize, Option<Reply>), BreakReason> {
        let mut used = 0;
        loop {
            match self.state {
                State::Length(0) => return Ok((used, Some(self.finish()))),
                State::Head | State::ChunkSize | State::Trailers => {
                    if used == input.len() {
                        return Ok((used, None));
                    }
                    // Copy a line at a time, retaining subsequent body/response bytes.
                    let rest = &input[used..];
                    let take = rest
                        .iter()
                        .position(|byte| *byte == b'\n')
                        .map_or(rest.len(), |end| end + 1);
                    self.line.try_reserve(take).map_err(|_| {
                        BreakReason::Resources("response header allocation")
                    })?;
                    self.line.extend_from_slice(&rest[..take]);
                    used += take;
                    let ready = if matches!(self.state, State::Head) {
                        self.line.ends_with(b"\r\n\r\n")
                    } else {
                        self.line.ends_with(b"\r\n")
                    };
                    if !ready {
                        continue;
                    }
                    match self.state {
                        State::Head => self.head()?,
                        State::ChunkSize => {
                            let httparse::Status::Complete((_, size)) =
                                httparse::parse_chunk_size(&self.line)
                                    .map_err(|_| {
                                        BreakReason::Protocol(
                                            "invalid chunk size",
                                        )
                                    })?
                            else {
                                return Err(BreakReason::Protocol(
                                    "incomplete chunk size line",
                                ));
                            };
                            self.line.clear();
                            self.state = if size == 0 {
                                State::Trailers
                            } else {
                                State::Chunk(size)
                            };
                        }
                        State::Trailers => {
                            let last = self.line == b"\r\n";
                            if !last {
                                self.line.try_reserve(2).map_err(|_| {
                                    BreakReason::Resources("trailer validation")
                                })?;
                                self.line.extend_from_slice(b"\r\n");
                                let mut headers = [httparse::EMPTY_HEADER];
                                let parsed = httparse::parse_headers(
                                    &self.line,
                                    &mut headers,
                                );
                                if !matches!(
                                    parsed,
                                    Ok(httparse::Status::Complete((_, [_])))
                                ) {
                                    return Err(BreakReason::Protocol(
                                        "invalid trailer",
                                    ));
                                }
                            }
                            self.line.clear();
                            if last {
                                return Ok((used, Some(self.finish())));
                            }
                        }
                        _ => unreachable!(),
                    }
                }
                State::Length(remaining) | State::Chunk(remaining) => {
                    if used == input.len() {
                        return Ok((used, None));
                    }
                    let size =
                        remaining.min((input.len() - used) as u64) as usize;
                    self.data(&input[used..used + size])?;
                    used += size;
                    self.state = match self.state {
                        State::Length(_) => {
                            State::Length(remaining - size as u64)
                        }
                        _ if remaining == size as u64 => State::ChunkEnd(0),
                        _ => State::Chunk(remaining - size as u64),
                    };
                }
                State::ChunkEnd(mut offset) => {
                    while offset < 2 && used < input.len() {
                        if input[used] != b"\r\n"[offset as usize] {
                            return Err(BreakReason::Protocol(
                                "invalid chunk terminator",
                            ));
                        }
                        offset += 1;
                        used += 1;
                    }
                    self.state = if offset == 2 {
                        State::ChunkSize
                    } else {
                        State::ChunkEnd(offset)
                    };
                    if used == input.len() {
                        return Ok((used, None));
                    }
                }
                State::UntilClose => {
                    self.data(&input[used..])?;
                    return Ok((input.len(), None));
                }
            }
        }
    }

    pub fn eof(&mut self) -> Option<Reply> {
        matches!(self.state, State::UntilClose).then(|| self.finish())
    }

    fn data(
        &mut self,
        data: &[u8],
    ) -> Result<(), BreakReason> {
        if let Some(expected) = &self.expected_body {
            let end = self
                .offset
                .checked_add(data.len())
                .ok_or(BreakReason::Protocol("body offset overflow"))?;
            self.matches &= expected.get(self.offset..end) == Some(data);
            self.offset = end;
        }
        Ok(())
    }

    fn finish(&mut self) -> Reply {
        let status_ok = (200..300).contains(&self.status)
            && self
                .expected_status
                .map_or((200..300).contains(&self.status), |status| {
                    status.as_u16() == self.status
                });
        let body_ok = self.matches
            && self
                .expected_body
                .as_ref()
                .is_none_or(|expected| expected.len() == self.offset);
        let outcome = if !status_ok {
            Outcome::Status
        } else if !body_ok {
            Outcome::Body
        } else {
            Outcome::Accepted
        };
        let reply = Reply { outcome, close: self.close };
        self.state = State::Head;
        self.offset = 0;
        self.matches = true;
        self.close = false;
        reply
    }

    fn head(&mut self) -> Result<(), BreakReason> {
        let mut small = [httparse::EMPTY_HEADER; 32];
        let head = match parse_head(&self.line, &mut small)? {
            Some(head) => head,
            None => {
                let mut fields = Vec::new();
                let mut count = 64_usize;
                loop {
                    fields.try_reserve(count).map_err(|_| {
                        BreakReason::Resources("response header fields")
                    })?;
                    fields.resize(count, httparse::EMPTY_HEADER);
                    if let Some(head) = parse_head(&self.line, &mut fields)? {
                        break head;
                    }
                    count = count.checked_mul(2).ok_or(
                        BreakReason::Resources("too many response fields"),
                    )?;
                }
            }
        };
        self.line.clear();
        self.status = head.status;
        self.close = head.close;
        self.state = head.state;
        Ok(())
    }
}

struct Head {
    status: u16,
    close: bool,
    state: State,
}

// None asks the caller for more header slots, without imposing a protocol limit.
fn parse_head<'a>(
    bytes: &'a [u8],
    headers: &mut [httparse::Header<'a>],
) -> Result<Option<Head>, BreakReason> {
    let mut response = httparse::Response::new(headers);
    match response.parse(bytes) {
        Err(httparse::Error::TooManyHeaders) => return Ok(None),
        Err(_) => return Err(BreakReason::Protocol("invalid response head")),
        Ok(httparse::Status::Partial) => {
            return Err(BreakReason::Protocol("incomplete response head"));
        }
        Ok(httparse::Status::Complete(_)) => {}
    }
    let status = response
        .code
        .ok_or(BreakReason::Protocol("missing response status"))?;
    if !(100..600).contains(&status) {
        return Err(BreakReason::Protocol("invalid response status"));
    }
    if status == 101 {
        return Err(BreakReason::Protocol(
            "protocol switching is not supported",
        ));
    }
    if status < 200 {
        return Ok(Some(Head { status, close: false, state: State::Head }));
    }
    let mut length = None;
    let mut chunked = false;
    let mut close = false;
    let mut keep_alive = false;
    for header in response.headers.iter() {
        if header.name.eq_ignore_ascii_case("content-length") {
            for value in header.value.split(|byte| *byte == b',') {
                let value = decimal(trim(value))
                    .ok_or(BreakReason::Protocol("invalid content-length"))?;
                if length.is_some_and(|old| old != value) {
                    return Err(BreakReason::Protocol(
                        "conflicting content-length",
                    ));
                }
                length = Some(value);
            }
        } else if header.name.eq_ignore_ascii_case("transfer-encoding") {
            if chunked || !trim(header.value).eq_ignore_ascii_case(b"chunked") {
                return Err(BreakReason::Protocol(
                    "unsupported transfer coding",
                ));
            }
            chunked = true;
        } else if header.name.eq_ignore_ascii_case("connection") {
            for value in header.value.split(|byte| *byte == b',') {
                close |= trim(value).eq_ignore_ascii_case(b"close");
                keep_alive |= trim(value).eq_ignore_ascii_case(b"keep-alive");
            }
        }
    }
    close |= response.version != Some(1) && !keep_alive;
    if chunked && length.is_some() {
        return Err(BreakReason::Protocol("ambiguous response framing"));
    }
    let state = if status == 204 || status == 304 {
        State::Length(0)
    } else if chunked {
        State::ChunkSize
    } else if let Some(size) = length {
        State::Length(size)
    } else {
        close = true;
        State::UntilClose
    };
    Ok(Some(Head { status, close, state }))
}

fn decimal(value: &[u8]) -> Option<u64> {
    if value.is_empty() {
        return None;
    }
    value.iter().try_fold(0_u64, |total, byte| {
        if !byte.is_ascii_digit() {
            return None;
        }
        total.checked_mul(10)?.checked_add(u64::from(byte - b'0'))
    })
}

fn trim(mut value: &[u8]) -> &[u8] {
    while value.first().is_some_and(|byte| matches!(byte, b' ' | b'\t')) {
        value = &value[1..];
    }
    while value.last().is_some_and(|byte| matches!(byte, b' ' | b'\t')) {
        value = &value[..value.len() - 1];
    }
    value
}

#[cfg(test)]
mod tests {
    use super::*;

    fn consume(
        decoder: &mut Decoder,
        mut bytes: &[u8],
        replies: &mut Vec<Reply>,
    ) -> Result<(), BreakReason> {
        while !bytes.is_empty() {
            let (used, reply) = decoder.feed(bytes)?;
            assert!(used > 0);
            bytes = &bytes[used..];
            replies.extend(reply);
        }
        Ok(())
    }

    #[test]
    fn arbitrary_fragmentation_preserves_informational_chunked_and_pipelined_replies()
     {
        let bytes = b"HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 201 Created\r\nTransfer-Encoding: chunked\r\n\r\n1\r\no\r\n1\r\nk\r\n0\r\nX-Test: yes\r\n\r\nHTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";
        for size in 1..=bytes.len() {
            let mut decoder =
                Decoder::new(None, Some(Bytes::from_static(b"ok")));
            let mut replies = Vec::new();
            for packet in bytes.chunks(size) {
                consume(&mut decoder, packet, &mut replies).unwrap();
            }
            assert_eq!(replies.len(), 2, "fragment size {size}");
            assert!(
                replies
                    .iter()
                    .all(|reply| matches!(reply.outcome, Outcome::Accepted))
            );
        }
    }

    #[test]
    fn malformed_lengths_and_trailers_are_never_accepted() {
        for bytes in [
            b"HTTP/1.1 200 OK\r\nContent-Length: +2\r\n\r\nok".as_slice(),
            b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nContent-Length: 3\r\n\r\nok",
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n0\r\nno-colon\r\n\r\n",
        ] {
            let mut decoder = Decoder::new(None, None);
            assert!(consume(&mut decoder, bytes, &mut Vec::new()).is_err());
        }
    }

    #[test]
    fn wide_headers_have_no_hidden_field_count_ceiling() {
        let mut bytes = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n".to_vec();
        for _ in 0..100 {
            bytes.extend_from_slice(b"X-Test: value\r\n");
        }
        bytes.extend_from_slice(b"\r\nok");
        let mut replies = Vec::new();
        consume(&mut Decoder::new(None, None), &bytes, &mut replies).unwrap();
        assert_eq!(replies.len(), 1);
        assert!(matches!(replies[0].outcome, Outcome::Accepted));
    }
}
