use std::convert::TryInto;
use std::io::{self, ErrorKind, IoSlice, IoSliceMut, Read, Write};
use std::mem::size_of;
use std::net::{self, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::os::windows::io::{AsRawSocket, FromRawSocket, IntoRawSocket, RawSocket};
use std::os::windows::raw::SOCKET as StdSocket;
use std::ptr;
use std::time::Duration; // winapi uses usize, stdlib uses u32/u64.

use winapi::ctypes::{c_char, c_int, c_ulong, c_ushort};
use winapi::shared::mstcpip;
use winapi::shared::ws2def::{AF_INET, AF_INET6, SOCKADDR_IN, SOCKADDR_STORAGE};
use winapi::shared::ws2ipdef::SOCKADDR_IN6_LH;

use winapi::shared::minwindef::{BOOL, DWORD, FALSE, LPDWORD, LPVOID, TRUE};
pub use winapi::um::winsock2::{
    self, closesocket, getsockname, getsockopt, linger, setsockopt, WSAIoctl, LPWSAOVERLAPPED,
    PF_INET, PF_INET6, SOCKET as TcpSocket, SOCKET_ERROR, SOCK_STREAM, SOL_SOCKET, SO_KEEPALIVE,
    SO_LINGER, SO_RCVBUF, SO_REUSEADDR, SO_SNDBUF,
};

use crate::net::TcpKeepalive;
use crate::sys::windows::from_raw_arc::FromRawArc;
use crate::sys::windows::net::{init, new_socket, socket_addr};
use crate::sys::windows::selector::{Overlapped, ReadyBinding};
use crate::sys::windows::{Family, Ready, Registration};
use crate::{event, Interest, Registry, Token};
use iovec::IoVec;
use log::trace;
use miow::iocp::CompletionStatus;
use miow::net::*;
use net2::{TcpBuilder, TcpStreamExt as Net2TcpExt};
use std::fmt;
use std::mem;
use std::net::Shutdown;
use std::sync::{Mutex, MutexGuard};
use winapi::um::minwinbase::OVERLAPPED_ENTRY;
use winapi::um::winnt::HANDLE;

pub struct TcpStream {
    /// Separately stored implementation to ensure that the `Drop`
    /// implementation on this type is only executed when it's actually dropped
    /// (many clones of this `imp` are made).
    imp: StreamImp,
    registration: Mutex<Option<Registration>>,
}

pub struct TcpListener {
    imp: ListenerImp,
    registration: Mutex<Option<Registration>>,
}

#[derive(Clone)]
struct StreamImp {
    /// A stable address and synchronized access for all internals. This serves
    /// to ensure that all `Overlapped` pointers are valid for a long period of
    /// time as well as allowing completion callbacks to have access to the
    /// internals without having ownership.
    ///
    /// Note that the reference count also allows us "loan out" copies to
    /// completion ports while I/O is running to guarantee that this stays alive
    /// until the I/O completes. You'll notice a number of calls to
    /// `mem::forget` below, and these only happen on successful scheduling of
    /// I/O and are paired with `overlapped2arc!` macro invocations in the
    /// completion callbacks (to have a decrement match the increment).
    inner: FromRawArc<StreamIo>,
}

#[derive(Clone)]
struct ListenerImp {
    inner: FromRawArc<ListenerIo>,
}

struct StreamIo {
    inner: Mutex<StreamInner>,
    read: Overlapped, // also used for connect
    write: Overlapped,
    socket: net::TcpStream,
}

struct ListenerIo {
    inner: Mutex<ListenerInner>,
    accept: Overlapped,
    family: Family,
    socket: net::TcpListener,
}

struct StreamInner {
    iocp: ReadyBinding,
    deferred_connect: Option<SocketAddr>,
    read: State<(), ()>,
    write: State<(Vec<u8>, usize), (Vec<u8>, usize)>,
    /// whether we are instantly notified of success
    /// (FILE_SKIP_COMPLETION_PORT_ON_SUCCESS,
    ///  without a roundtrip through the event loop)
    instant_notify: bool,
}

struct ListenerInner {
    iocp: ReadyBinding,
    accept: State<net::TcpStream, (net::TcpStream, SocketAddr)>,
    accept_buf: AcceptAddrsBuf,
    // While compiling with target x86_64-uwp-windows-msvc,
    // the field is mistakenly reported as unused.
    #[allow(dead_code)]
    instant_notify: bool,
}

enum State<T, U> {
    Empty,            // no I/O operation in progress
    Pending(T),       // an I/O operation is in progress
    Ready(U),         // I/O has finished with this value
    Error(io::Error), // there was an I/O error
}

impl TcpStream {
    fn new(socket: net::TcpStream, deferred_connect: Option<SocketAddr>) -> TcpStream {
        TcpStream {
            registration: Mutex::new(None),
            imp: StreamImp {
                inner: FromRawArc::new(StreamIo {
                    read: Overlapped::new(read_done),
                    write: Overlapped::new(write_done),
                    socket: socket,
                    inner: Mutex::new(StreamInner {
                        iocp: ReadyBinding::new(),
                        deferred_connect: deferred_connect,
                        read: State::Empty,
                        write: State::Empty,
                        instant_notify: false,
                    }),
                }),
            },
        }
    }

    pub(crate) fn connect_mio(socket: TcpSocket, addr: SocketAddr) -> io::Result<TcpStream> {
        // TODO: Safety
        let os_socket = unsafe { ::std::net::TcpStream::from_raw_socket(socket as RawSocket) };
        os_socket.set_nonblocking(true)?;
        Ok(TcpStream::new(os_socket, Some(addr)))
        // let socket = os_socket.into_raw_socket();
        // connect(socket as usize, addr).map(|s| TcpStream::new(s, Some(addr)))
    }

    pub fn connect(addr: SocketAddr) -> io::Result<TcpStream> {
        let socket = new_for_addr(addr)?;
        // TODO: Safety
        let os_socket = unsafe { ::std::net::TcpStream::from_raw_socket(socket as RawSocket) };
        os_socket.set_nonblocking(true)?;
        let socket = os_socket.into_raw_socket();
        connect(socket as usize, addr).map(|s| TcpStream::new(s, Some(addr)))
    }

    pub fn from_std(stream: net::TcpStream) -> TcpStream {
        TcpStream::new(stream, None)
    }

    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.imp.inner.socket.peer_addr()
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.imp.inner.socket.local_addr()
    }

    pub fn try_clone(&self) -> io::Result<TcpStream> {
        self.imp
            .inner
            .socket
            .try_clone()
            .map(|s| TcpStream::new(s, None))
    }

    pub fn shutdown(&self, how: Shutdown) -> io::Result<()> {
        self.imp.inner.socket.shutdown(how)
    }

    pub fn set_nodelay(&self, nodelay: bool) -> io::Result<()> {
        self.imp.inner.socket.set_nodelay(nodelay)
    }

    pub fn nodelay(&self) -> io::Result<bool> {
        self.imp.inner.socket.nodelay()
    }

    pub fn set_recv_buffer_size(&self, size: usize) -> io::Result<()> {
        self.imp.inner.socket.set_recv_buffer_size(size)
    }

    pub fn recv_buffer_size(&self) -> io::Result<usize> {
        self.imp.inner.socket.recv_buffer_size()
    }

    pub fn set_send_buffer_size(&self, size: usize) -> io::Result<()> {
        self.imp.inner.socket.set_send_buffer_size(size)
    }

    pub fn send_buffer_size(&self) -> io::Result<usize> {
        self.imp.inner.socket.send_buffer_size()
    }

    pub fn set_keepalive(&self, keepalive: Option<Duration>) -> io::Result<()> {
        self.imp.inner.socket.set_keepalive(keepalive)
    }

    pub fn keepalive(&self) -> io::Result<Option<Duration>> {
        self.imp.inner.socket.keepalive()
    }

    pub fn set_ttl(&self, ttl: u32) -> io::Result<()> {
        self.imp.inner.socket.set_ttl(ttl)
    }

    pub fn ttl(&self) -> io::Result<u32> {
        self.imp.inner.socket.ttl()
    }

    pub fn set_linger(&self, dur: Option<Duration>) -> io::Result<()> {
        net2::TcpStreamExt::set_linger(&self.imp.inner.socket, dur)
    }

    pub fn linger(&self) -> io::Result<Option<Duration>> {
        net2::TcpStreamExt::linger(&self.imp.inner.socket)
    }

    pub fn take_error(&self) -> io::Result<Option<io::Error>> {
        if let Some(e) = self.imp.inner.socket.take_error()? {
            return Ok(Some(e));
        }

        // If the syscall didn't return anything then also check to see if we've
        // squirreled away an error elsewhere for example as part of a connect
        // operation.
        //
        // Typically this is used like so:
        //
        // 1. A `connect` is issued
        // 2. Wait for the socket to be writable
        // 3. Call `take_error` to see if the connect succeeded.
        //
        // Right now the `connect` operation finishes in `read_done` below and
        // fill will in `State::Error` in the `read` slot if it fails, so we
        // extract that here.
        let mut me = self.inner();
        match mem::replace(&mut me.read, State::Empty) {
            State::Error(e) => {
                self.imp.schedule_read(&mut me);
                Ok(Some(e))
            }
            other => {
                me.read = other;
                Ok(None)
            }
        }
    }

    fn inner(&self) -> MutexGuard<'_, StreamInner> {
        self.imp.inner()
    }

    fn before_read(&self) -> io::Result<MutexGuard<'_, StreamInner>> {
        let mut me = self.inner();

        match me.read {
            // Empty == we're not associated yet, and if we're pending then
            // these are both cases where we return "would block"
            State::Empty | State::Pending(()) => return Err(io::ErrorKind::WouldBlock.into()),

            // If we got a delayed error as part of a `read_overlapped` below,
            // return that here. Also schedule another read in case it was
            // transient.
            State::Error(_) => {
                let e = match mem::replace(&mut me.read, State::Empty) {
                    State::Error(e) => e,
                    _ => panic!(),
                };
                self.imp.schedule_read(&mut me);
                return Err(e);
            }

            // If we're ready for a read then some previous 0-byte read has
            // completed. In that case the OS's socket buffer has something for
            // us, so we just keep pulling out bytes while we can in the loop
            // below.
            State::Ready(()) => {}
        }

        Ok(me)
    }

    fn post_register(&self, interests: Interest, me: &mut StreamInner) {
        if interests.is_readable() {
            self.imp.schedule_read(me);
        }

        // At least with epoll, if a socket is registered with an interest in
        // writing and it's immediately writable then a writable event is
        // generated immediately, so do so here.
        if interests.is_writable() {
            if let State::Empty = me.write {
                self.imp.add_readiness(me, Ready::WRITABLE);
            }
        }
    }

    pub fn peek(&self, buf: &mut [u8]) -> io::Result<usize> {
        let mut me = self.before_read()?;

        match (&self.imp.inner.socket).peek(buf) {
            Ok(n) => Ok(n),
            Err(e) => {
                me.read = State::Empty;
                self.imp.schedule_read(&mut me);
                Err(e)
            }
        }
    }

    pub fn write(&self, buf: &[u8]) -> io::Result<usize> {
        match IoVec::from_bytes(buf) {
            Some(vec) => self.writev(&[vec]),
            None => Ok(0),
        }
    }

    pub fn writev(&self, bufs: &[&IoVec]) -> io::Result<usize> {
        let mut me = self.inner();
        let me = &mut *me;

        match mem::replace(&mut me.write, State::Empty) {
            State::Empty => {}
            State::Error(e) => return Err(e),
            other => {
                me.write = other;
                return Err(io::ErrorKind::WouldBlock.into());
            }
        }

        if !me.iocp.registered() {
            return Err(io::ErrorKind::WouldBlock.into());
        }

        if bufs.is_empty() {
            return Ok(0);
        }

        let len = bufs.iter().map(|b| b.len()).fold(0, |a, b| a + b);
        let mut intermediate = me.iocp.get_buffer(len);
        for buf in bufs {
            intermediate.extend_from_slice(buf);
        }
        self.imp.schedule_write(intermediate, 0, me);
        Ok(len)
    }
}

impl Read for TcpStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.read_vectored(&mut [IoSliceMut::new(buf)][..])
    }

    fn read_vectored(&mut self, bufs: &mut [IoSliceMut<'_>]) -> io::Result<usize> {
        let mut me = self.before_read()?;

        // TODO: Does WSARecv work on a nonblocking sockets? We ideally want to
        //       call that instead of looping over all the buffers and calling
        //       `recv` on each buffer. I'm not sure though if an overlapped
        //       socket in nonblocking mode would work with that use case,
        //       however, so for now we just call `recv`.

        let mut amt = 0;
        for buf in bufs {
            match (&self.imp.inner.socket).read(buf) {
                // If we did a partial read, then return what we've read so far
                Ok(n) if n < buf.len() => return Ok(amt + n),

                // Otherwise filled this buffer entirely, so try to fill the
                // next one as well.
                Ok(n) => amt += n,

                // If we hit an error then things get tricky if we've already
                // read some data. If the error is "would block" then we just
                // return the data we've read so far while scheduling another
                // 0-byte read.
                //
                // If we've read data and the error kind is not "would block",
                // then we stash away the error to get returned later and return
                // the data that we've read.
                //
                // Finally if we haven't actually read any data we just
                // reschedule a 0-byte read to happen again and then return the
                // error upwards.
                Err(e) => {
                    if amt > 0 && e.kind() == io::ErrorKind::WouldBlock {
                        me.read = State::Empty;
                        self.imp.schedule_read(&mut me);
                        return Ok(amt);
                    } else if amt > 0 {
                        me.read = State::Error(e);
                        return Ok(amt);
                    } else {
                        me.read = State::Empty;
                        self.imp.schedule_read(&mut me);
                        return Err(e);
                    }
                }
            }
        }

        Ok(amt)
    }
}

impl<'a> Read for &'a TcpStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.read_vectored(&mut [IoSliceMut::new(buf)][..])
    }

    fn read_vectored(&mut self, bufs: &mut [IoSliceMut<'_>]) -> io::Result<usize> {
        let mut me = self.before_read()?;

        // TODO: Does WSARecv work on a nonblocking sockets? We ideally want to
        //       call that instead of looping over all the buffers and calling
        //       `recv` on each buffer. I'm not sure though if an overlapped
        //       socket in nonblocking mode would work with that use case,
        //       however, so for now we just call `recv`.

        let mut amt = 0;
        for buf in bufs {
            match (&self.imp.inner.socket).read(buf) {
                // If we did a partial read, then return what we've read so far
                Ok(n) if n < buf.len() => return Ok(amt + n),

                // Otherwise filled this buffer entirely, so try to fill the
                // next one as well.
                Ok(n) => amt += n,

                // If we hit an error then things get tricky if we've already
                // read some data. If the error is "would block" then we just
                // return the data we've read so far while scheduling another
                // 0-byte read.
                //
                // If we've read data and the error kind is not "would block",
                // then we stash away the error to get returned later and return
                // the data that we've read.
                //
                // Finally if we haven't actually read any data we just
                // reschedule a 0-byte read to happen again and then return the
                // error upwards.
                Err(e) => {
                    if amt > 0 && e.kind() == io::ErrorKind::WouldBlock {
                        me.read = State::Empty;
                        self.imp.schedule_read(&mut me);
                        return Ok(amt);
                    } else if amt > 0 {
                        me.read = State::Error(e);
                        return Ok(amt);
                    } else {
                        me.read = State::Empty;
                        self.imp.schedule_read(&mut me);
                        return Err(e);
                    }
                }
            }
        }

        Ok(amt)
    }
}

impl Write for TcpStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.write_vectored(&[IoSlice::new(buf)][..])
    }

    fn write_vectored(&mut self, bufs: &[IoSlice<'_>]) -> io::Result<usize> {
        let mut me = self.inner();
        let me = &mut *me;

        match mem::replace(&mut me.write, State::Empty) {
            State::Empty => {}
            State::Error(e) => return Err(e),
            other => {
                me.write = other;
                return Err(io::ErrorKind::WouldBlock.into());
            }
        }

        if !me.iocp.registered() {
            return Err(io::ErrorKind::WouldBlock.into());
        }

        if bufs.is_empty() {
            return Ok(0);
        }

        let len = bufs.iter().map(|b| b.len()).fold(0, |a, b| a + b);
        let mut intermediate = me.iocp.get_buffer(len);
        for buf in bufs {
            intermediate.extend_from_slice(buf);
        }
        self.imp.schedule_write(intermediate, 0, me);
        Ok(len)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> Write for &'a TcpStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.write_vectored(&[IoSlice::new(buf)][..])
    }

    fn write_vectored(&mut self, bufs: &[IoSlice<'_>]) -> io::Result<usize> {
        let mut me = self.inner();
        let me = &mut *me;

        match mem::replace(&mut me.write, State::Empty) {
            State::Empty => {}
            State::Error(e) => return Err(e),
            other => {
                me.write = other;
                return Err(io::ErrorKind::WouldBlock.into());
            }
        }

        if !me.iocp.registered() {
            return Err(io::ErrorKind::WouldBlock.into());
        }

        if bufs.is_empty() {
            return Ok(0);
        }

        let len = bufs.iter().map(|b| b.len()).fold(0, |a, b| a + b);
        let mut intermediate = me.iocp.get_buffer(len);
        for buf in bufs {
            intermediate.extend_from_slice(buf);
        }
        self.imp.schedule_write(intermediate, 0, me);
        Ok(len)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl StreamImp {
    fn inner(&self) -> MutexGuard<'_, StreamInner> {
        self.inner.inner.lock().unwrap()
    }

    fn schedule_connect(&self, addr: SocketAddr) -> io::Result<()> {
        unsafe {
            trace!("scheduling a connect");
            self.inner
                .socket
                .connect_overlapped(&addr, &[], self.inner.read.as_mut_ptr())?;
        }
        // see docs above on StreamImp.inner for rationale on forget
        mem::forget(self.clone());
        Ok(())
    }

    /// Schedule a read to happen on this socket, enqueuing us to receive a
    /// notification when a read is ready.
    ///
    /// Note that this does *not* work with a buffer. When reading a TCP stream
    /// we actually read into a 0-byte buffer so Windows will send us a
    /// notification when the socket is otherwise ready for reading. This allows
    /// us to avoid buffer allocations for in-flight reads.
    fn schedule_read(&self, me: &mut StreamInner) {
        match me.read {
            State::Empty => {}
            State::Ready(_) | State::Error(_) => {
                self.add_readiness(me, Ready::READABLE);
                return;
            }
            _ => return,
        }

        me.iocp.set_readiness(me.iocp.readiness() - Ready::READABLE);

        trace!("scheduling a read");
        let res = unsafe {
            self.inner
                .socket
                .read_overlapped(&mut [], self.inner.read.as_mut_ptr())
        };
        match res {
            // Note that `Ok(true)` means that this completed immediately and
            // our socket is readable. This typically means that the caller of
            // this function (likely `read` above) can try again as an
            // optimization and return bytes quickly.
            //
            // Normally, though, although the read completed immediately
            // there's still an IOCP completion packet enqueued that we're going
            // to receive.
            //
            // You can configure this behavior (miow) with
            // SetFileCompletionNotificationModes to indicate that `Ok(true)`
            // does **not** enqueue a completion packet. (This is the case
            // for me.instant_notify)
            //
            // Note that apparently libuv has scary code to work around bugs in
            // `WSARecv` for UDP sockets apparently for handles which have had
            // the `SetFileCompletionNotificationModes` function called on them,
            // worth looking into!
            Ok(Some(_)) if me.instant_notify => {
                me.read = State::Ready(());
                self.add_readiness(me, Ready::READABLE);
            }
            Ok(_) => {
                // see docs above on StreamImp.inner for rationale on forget
                me.read = State::Pending(());
                mem::forget(self.clone());
            }
            Err(e) => {
                me.read = State::Error(e);
                self.add_readiness(me, Ready::READABLE);
            }
        }
    }

    /// Similar to `schedule_read`, except that this issues, well, writes.
    ///
    /// This function will continually attempt to write the entire contents of
    /// the buffer `buf` until they have all been written. The `pos` argument is
    /// the current offset within the buffer up to which the contents have
    /// already been written.
    ///
    /// A new writable event (e.g. allowing another write) will only happen once
    /// the buffer has been written completely (or hit an error).
    fn schedule_write(&self, buf: Vec<u8>, mut pos: usize, me: &mut StreamInner) {
        // About to write, clear any pending level triggered events
        me.iocp.set_readiness(me.iocp.readiness() - Ready::WRITABLE);

        loop {
            trace!("scheduling a write of {} bytes", buf[pos..].len());
            let ret = unsafe {
                self.inner
                    .socket
                    .write_overlapped(&buf[pos..], self.inner.write.as_mut_ptr())
            };
            match ret {
                Ok(Some(transferred_bytes)) if me.instant_notify => {
                    trace!("done immediately with {} bytes", transferred_bytes);
                    if transferred_bytes == buf.len() - pos {
                        self.add_readiness(me, Ready::WRITABLE);
                        me.write = State::Empty;
                        break;
                    }
                    pos += transferred_bytes;
                }
                Ok(_) => {
                    trace!("scheduled for later");
                    // see docs above on StreamImp.inner for rationale on forget
                    me.write = State::Pending((buf, pos));
                    mem::forget(self.clone());
                    break;
                }
                Err(e) => {
                    trace!("write error: {}", e);
                    me.write = State::Error(e);
                    self.add_readiness(me, Ready::WRITABLE);
                    me.iocp.put_buffer(buf);
                    break;
                }
            }
        }
    }

    /// Pushes an event for this socket onto the selector its registered for.
    ///
    /// When an event is generated on this socket, if it happened after the
    /// socket was closed then we don't want to actually push the event onto our
    /// selector as otherwise it's just a spurious notification.
    fn add_readiness(&self, me: &mut StreamInner, set: Ready) {
        me.iocp.set_readiness(set | me.iocp.readiness());
    }
}

fn read_done(status: &OVERLAPPED_ENTRY) {
    let status = CompletionStatus::from_entry(status);
    let me2 = StreamImp {
        inner: unsafe { overlapped2arc!(status.overlapped(), StreamIo, read) },
    };

    let mut me = me2.inner();
    match mem::replace(&mut me.read, State::Empty) {
        State::Pending(()) => {
            trace!("finished a read: {}", status.bytes_transferred());
            assert_eq!(status.bytes_transferred(), 0);
            me.read = State::Ready(());
            return me2.add_readiness(&mut me, Ready::READABLE);
        }
        s => me.read = s,
    }

    // If a read didn't complete, then the connect must have just finished.
    trace!("finished a connect");

    // By guarding with socket.result(), we ensure that a connection
    // was successfully made before performing operations requiring a
    // connected socket.
    match unsafe { me2.inner.socket.result(status.overlapped()) }
        .and_then(|_| me2.inner.socket.connect_complete())
    {
        Ok(()) => {
            me2.add_readiness(&mut me, Ready::WRITABLE);
            me2.schedule_read(&mut me);
        }
        Err(e) => {
            me2.add_readiness(&mut me, Ready::READABLE | Ready::WRITABLE);
            me.read = State::Error(e);
        }
    }
}

fn write_done(status: &OVERLAPPED_ENTRY) {
    let status = CompletionStatus::from_entry(status);
    trace!("finished a write {}", status.bytes_transferred());
    let me2 = StreamImp {
        inner: unsafe { overlapped2arc!(status.overlapped(), StreamIo, write) },
    };
    let mut me = me2.inner();
    let (buf, pos) = match mem::replace(&mut me.write, State::Empty) {
        State::Pending(pair) => pair,
        _ => unreachable!(),
    };
    let new_pos = pos + (status.bytes_transferred() as usize);
    if new_pos == buf.len() {
        me2.add_readiness(&mut me, Ready::WRITABLE);
    } else {
        me2.schedule_write(buf, new_pos, &mut me);
    }
}

impl event::Source for TcpStream {
    fn register(
        &mut self,
        registry: &Registry,
        token: Token,
        interests: Interest,
    ) -> io::Result<()> {
        let mut me = self.inner();
        me.iocp.register_socket(
            &self.imp.inner.socket,
            registry,
            token,
            interests,
            &self.registration,
        )?;

        unsafe {
            super::no_notify_on_instant_completion(self.imp.inner.socket.as_raw_socket() as HANDLE)?;
            me.instant_notify = true;
        }

        // If we were connected before being registered process that request
        // here and go along our merry ways. Note that the callback for a
        // successful connect will worry about generating writable/readable
        // events and scheduling a new read.
        if let Some(addr) = me.deferred_connect.take() {
            return self.imp.schedule_connect(addr).map(|_| ());
        }
        self.post_register(interests, &mut me);
        Ok(())
    }

    fn reregister(
        &mut self,
        registry: &Registry,
        token: Token,
        interests: Interest,
    ) -> io::Result<()> {
        let mut me = self.inner();
        me.iocp.reregister_socket(
            &self.imp.inner.socket,
            registry,
            token,
            interests,
            &self.registration,
        )?;
        self.post_register(interests, &mut me);
        Ok(())
    }

    fn deregister(&mut self, registry: &Registry) -> io::Result<()> {
        let StreamIo { inner, socket, .. } = &mut *self.imp.inner;
        inner
            .lock()
            .unwrap()
            .iocp
            .deregister(socket, registry, &self.registration)
    }
}

impl fmt::Debug for TcpStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TcpStream").finish()
    }
}

impl IntoRawSocket for TcpStream {
    fn into_raw_socket(self) -> RawSocket {
        self.imp
            .inner
            .socket
            .try_clone()
            .expect("Cannot clone socket")
            .into_raw_socket()
    }
}

impl AsRawSocket for TcpStream {
    fn as_raw_socket(&self) -> RawSocket {
        self.imp.inner.socket.as_raw_socket()
    }
}

impl FromRawSocket for TcpStream {
    /// Converts a `RawSocket` to a `TcpStream`.
    ///
    /// # Notes
    ///
    /// The caller is responsible for ensuring that the socket is in
    /// non-blocking mode.
    unsafe fn from_raw_socket(socket: RawSocket) -> TcpStream {
        TcpStream::from_std(FromRawSocket::from_raw_socket(socket))
    }
}

impl Drop for TcpStream {
    fn drop(&mut self) {
        // If we're still internally reading, we're no longer interested. Note
        // though that we don't cancel any writes which may have been issued to
        // preserve the same semantics as Unix.
        //
        // Note that "Empty" here may mean that a connect is pending, so we
        // cancel even if that happens as well.
        unsafe {
            match self.inner().read {
                State::Pending(_) | State::Empty => {
                    trace!("cancelling active TCP read");
                    drop(super::cancel(&self.imp.inner.socket, &self.imp.inner.read));
                }
                State::Ready(_) | State::Error(_) => {}
            }
        }
    }
}

impl TcpListener {
    /// Convenience method to bind a new TCP listener to the specified address
    /// to receive new connections.
    ///
    /// This function will take the following steps:
    ///
    /// 1. Create a new TCP socket.
    /// 2. Set the `SO_REUSEADDR` option on the socket on Unix.
    /// 3. Bind the socket to the specified address.
    /// 4. Calls `listen` on the socket to prepare it to receive new connections.
    pub fn bind(addr: SocketAddr) -> io::Result<TcpListener> {
        let socket = new_for_addr(addr)?;

        // On platforms with Berkeley-derived sockets, this allows to quickly
        // rebind a socket, without needing to wait for the OS to clean up the
        // previous one.
        //
        // On Windows, this allows rebinding sockets which are actively in use,
        // which allows “socket hijacking”, so we explicitly don't set it here.
        // https://docs.microsoft.com/en-us/windows/win32/winsock/using-so-reuseaddr-and-so-exclusiveaddruse
        #[cfg(not(windows))]
        socket.set_reuseaddr(true)?;

        bind(socket, addr)?;
        listen(socket, 1024).map(TcpListener::from_std)
    }

    pub fn from_std(socket: net::TcpListener) -> TcpListener {
        let addr = socket.local_addr().unwrap();
        TcpListener::new_family(
            socket,
            match addr {
                SocketAddr::V4(..) => Family::V4,
                SocketAddr::V6(..) => Family::V6,
            },
        )
    }

    fn new_family(socket: net::TcpListener, family: Family) -> TcpListener {
        TcpListener {
            registration: Mutex::new(None),
            imp: ListenerImp {
                inner: FromRawArc::new(ListenerIo {
                    accept: Overlapped::new(accept_done),
                    family: family,
                    socket: socket,
                    inner: Mutex::new(ListenerInner {
                        iocp: ReadyBinding::new(),
                        accept: State::Empty,
                        accept_buf: AcceptAddrsBuf::new(),
                        instant_notify: false,
                    }),
                }),
            },
        }
    }

    pub fn accept(&self) -> io::Result<(TcpStream, SocketAddr)> {
        let mut me = self.inner();

        let ret = match mem::replace(&mut me.accept, State::Empty) {
            State::Empty => return Err(io::ErrorKind::WouldBlock.into()),
            State::Pending(t) => {
                me.accept = State::Pending(t);
                return Err(io::ErrorKind::WouldBlock.into());
            }
            State::Ready((s, a)) => Ok((s, a)),
            State::Error(e) => Err(e),
        };

        self.imp.schedule_accept(&mut me);

        ret.map(|(s, addr)| (TcpStream::from_std(s), addr))
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.imp.inner.socket.local_addr()
    }

    pub fn try_clone(&self) -> io::Result<TcpListener> {
        self.imp
            .inner
            .socket
            .try_clone()
            .map(|s| TcpListener::new_family(s, self.imp.inner.family))
    }

    pub fn set_ttl(&self, ttl: u32) -> io::Result<()> {
        self.imp.inner.socket.set_ttl(ttl)
    }

    pub fn ttl(&self) -> io::Result<u32> {
        self.imp.inner.socket.ttl()
    }

    pub fn take_error(&self) -> io::Result<Option<io::Error>> {
        self.imp.inner.socket.take_error()
    }

    fn inner(&self) -> MutexGuard<'_, ListenerInner> {
        self.imp.inner()
    }
}

impl ListenerImp {
    fn inner(&self) -> MutexGuard<'_, ListenerInner> {
        self.inner.inner.lock().unwrap()
    }

    fn schedule_accept(&self, me: &mut ListenerInner) {
        match me.accept {
            State::Empty => {}
            _ => return,
        }

        me.iocp.set_readiness(me.iocp.readiness() - Ready::READABLE);

        let res = match self.inner.family {
            Family::V4 => TcpBuilder::new_v4(),
            Family::V6 => TcpBuilder::new_v6(),
        }
        .and_then(|builder| unsafe {
            trace!("scheduling an accept");
            let socket = builder.to_tcp_stream()?;
            let ready = self.inner.socket.accept_overlapped(
                &socket,
                &mut me.accept_buf,
                self.inner.accept.as_mut_ptr(),
            )?;
            Ok((socket, ready))
        });
        match res {
            Ok((socket, _)) => {
                // see docs above on StreamImp.inner for rationale on forget
                me.accept = State::Pending(socket);
                mem::forget(self.clone());
            }
            Err(e) => {
                me.accept = State::Error(e);
                self.add_readiness(me, Ready::READABLE);
            }
        }
    }

    // See comments in StreamImp::push
    fn add_readiness(&self, me: &mut ListenerInner, set: Ready) {
        me.iocp.set_readiness(set | me.iocp.readiness());
    }
}

fn accept_done(status: &OVERLAPPED_ENTRY) {
    let status = CompletionStatus::from_entry(status);
    let me2 = ListenerImp {
        inner: unsafe { overlapped2arc!(status.overlapped(), ListenerIo, accept) },
    };

    let mut me = me2.inner();
    let socket = match mem::replace(&mut me.accept, State::Empty) {
        State::Pending(s) => s,
        _ => unreachable!(),
    };
    trace!("finished an accept");
    let result = me2
        .inner
        .socket
        .accept_complete(&socket)
        .and_then(|()| me.accept_buf.parse(&me2.inner.socket))
        .and_then(|buf| {
            buf.remote()
                .ok_or_else(|| io::Error::new(ErrorKind::Other, "could not obtain remote address"))
        });
    me.accept = match result {
        Ok(remote_addr) => State::Ready((socket, remote_addr)),
        Err(e) => State::Error(e),
    };
    me2.add_readiness(&mut me, Ready::READABLE);
}

impl event::Source for TcpListener {
    fn register(
        &mut self,
        registry: &Registry,
        token: Token,
        interests: Interest,
    ) -> io::Result<()> {
        let mut me = self.inner();
        me.iocp.register_socket(
            &self.imp.inner.socket,
            registry,
            token,
            interests,
            &self.registration,
        )?;

        unsafe {
            super::no_notify_on_instant_completion(self.imp.inner.socket.as_raw_socket() as HANDLE)?;
            me.instant_notify = true;
        }

        self.imp.schedule_accept(&mut me);
        Ok(())
    }

    fn reregister(
        &mut self,
        registry: &Registry,
        token: Token,
        interests: Interest,
    ) -> io::Result<()> {
        let mut me = self.inner();
        me.iocp.reregister_socket(
            &self.imp.inner.socket,
            registry,
            token,
            interests,
            &self.registration,
        )?;
        self.imp.schedule_accept(&mut me);
        Ok(())
    }

    fn deregister(&mut self, registry: &Registry) -> io::Result<()> {
        let ListenerIo { inner, socket, .. } = &mut *self.imp.inner;
        inner
            .lock()
            .unwrap()
            .iocp
            .deregister(socket, registry, &self.registration)
    }
}

impl fmt::Debug for TcpListener {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TcpListener").finish()
    }
}

impl IntoRawSocket for TcpListener {
    fn into_raw_socket(self) -> RawSocket {
        self.imp
            .inner
            .socket
            .try_clone()
            .expect("Cannot clone socket")
            .into_raw_socket()
    }
}

impl AsRawSocket for TcpListener {
    fn as_raw_socket(&self) -> RawSocket {
        self.imp.inner.socket.as_raw_socket()
    }
}

impl FromRawSocket for TcpListener {
    /// Converts a `RawSocket` to a `TcpListener`.
    ///
    /// # Notes
    ///
    /// The caller is responsible for ensuring that the socket is in
    /// non-blocking mode.
    unsafe fn from_raw_socket(socket: RawSocket) -> TcpListener {
        TcpListener::from_std(FromRawSocket::from_raw_socket(socket))
    }
}

impl Drop for TcpListener {
    fn drop(&mut self) {
        // If we're still internally reading, we're no longer interested.
        unsafe {
            match self.inner().accept {
                State::Pending(_) => {
                    trace!("cancelling active TCP accept");
                    drop(super::cancel(
                        &self.imp.inner.socket,
                        &self.imp.inner.accept,
                    ));
                }
                State::Empty | State::Ready(_) | State::Error(_) => {}
            }
        }
    }
}

pub(crate) fn new_for_addr(addr: SocketAddr) -> io::Result<TcpSocket> {
    if addr.is_ipv4() {
        new_v4_socket()
    } else {
        new_v6_socket()
    }
}

pub(crate) fn new_v4_socket() -> io::Result<TcpSocket> {
    init();
    new_socket(PF_INET, SOCK_STREAM).map(From::from)
}

pub(crate) fn new_v6_socket() -> io::Result<TcpSocket> {
    init();
    new_socket(PF_INET6, SOCK_STREAM).map(From::from)
}

pub(crate) fn bind(socket: TcpSocket, addr: SocketAddr) -> io::Result<()> {
    use winsock2::bind;

    let (raw_addr, raw_addr_length) = socket_addr(&addr);
    syscall!(
        bind(socket, raw_addr.as_ptr(), raw_addr_length),
        PartialEq::eq,
        SOCKET_ERROR
    )?;
    Ok(())
}

pub(crate) fn connect(socket: TcpSocket, addr: SocketAddr) -> io::Result<net::TcpStream> {
    use winsock2::connect;

    let (raw_addr, raw_addr_length) = socket_addr(&addr);

    let res = syscall!(
        connect(socket, raw_addr.as_ptr(), raw_addr_length),
        PartialEq::eq,
        SOCKET_ERROR
    );

    match res {
        Err(err) if err.kind() != io::ErrorKind::WouldBlock => Err(err),
        _ => Ok(unsafe { net::TcpStream::from_raw_socket(socket as StdSocket) }),
    }
}

pub(crate) fn listen(socket: TcpSocket, backlog: u32) -> io::Result<net::TcpListener> {
    use std::convert::TryInto;
    use WinSock::listen;

    let backlog = backlog.try_into().unwrap_or(i32::max_value());
    syscall!(listen(socket, backlog), PartialEq::eq, SOCKET_ERROR)?;
    Ok(unsafe { net::TcpListener::from_raw_socket(socket as StdSocket) })
}

pub(crate) fn close(socket: TcpSocket) {
    let _ = unsafe { closesocket(socket) };
}

pub(crate) fn set_reuseaddr(socket: TcpSocket, reuseaddr: bool) -> io::Result<()> {
    let val: BOOL = if reuseaddr { TRUE } else { FALSE };

    match unsafe {
        setsockopt(
            socket,
            SOL_SOCKET,
            SO_REUSEADDR,
            &val as *const _ as *const c_char,
            size_of::<BOOL>() as c_int,
        )
    } {
        SOCKET_ERROR => Err(io::Error::last_os_error()),
        _ => Ok(()),
    }
}

pub(crate) fn get_reuseaddr(socket: TcpSocket) -> io::Result<bool> {
    let mut optval: c_char = 0;
    let mut optlen = size_of::<BOOL>() as c_int;

    match unsafe {
        getsockopt(
            socket,
            SOL_SOCKET,
            SO_REUSEADDR,
            &mut optval as *mut _ as *mut _,
            &mut optlen,
        )
    } {
        SOCKET_ERROR => Err(io::Error::last_os_error()),
        _ => Ok(optval != 0),
    }
}

pub(crate) fn get_localaddr(socket: TcpSocket) -> io::Result<SocketAddr> {
    let mut storage: SOCKADDR_STORAGE = unsafe { std::mem::zeroed() };
    let mut length = std::mem::size_of_val(&storage) as c_int;

    match unsafe { getsockname(socket, &mut storage as *mut _ as *mut _, &mut length) } {
        SOCKET_ERROR => Err(io::Error::last_os_error()),
        _ => {
            if storage.ss_family as c_int == AF_INET {
                // Safety: if the ss_family field is AF_INET then storage must be a sockaddr_in.
                let addr: &SOCKADDR_IN = unsafe { &*(&storage as *const _ as *const SOCKADDR_IN) };
                let ip_bytes = unsafe { addr.sin_addr.S_un.S_un_b() };
                let ip =
                    Ipv4Addr::from([ip_bytes.s_b1, ip_bytes.s_b2, ip_bytes.s_b3, ip_bytes.s_b4]);
                let port = u16::from_be(addr.sin_port);
                Ok(SocketAddr::V4(SocketAddrV4::new(ip, port)))
            } else if storage.ss_family as c_int == AF_INET6 {
                // Safety: if the ss_family field is AF_INET6 then storage must be a sockaddr_in6.
                let addr: &SOCKADDR_IN6_LH =
                    unsafe { &*(&storage as *const _ as *const SOCKADDR_IN6_LH) };
                let ip = Ipv6Addr::from(*unsafe { addr.sin6_addr.u.Byte() });
                let port = u16::from_be(addr.sin6_port);
                let scope_id = unsafe { *addr.u.sin6_scope_id() };
                Ok(SocketAddr::V6(SocketAddrV6::new(
                    ip,
                    port,
                    addr.sin6_flowinfo,
                    scope_id,
                )))
            } else {
                Err(std::io::ErrorKind::InvalidInput.into())
            }
        }
    }
}

pub(crate) fn set_linger(socket: TcpSocket, dur: Option<Duration>) -> io::Result<()> {
    let val: linger = linger {
        l_onoff: if dur.is_some() { 1 } else { 0 },
        l_linger: dur.map(|dur| dur.as_secs() as c_ushort).unwrap_or_default(),
    };

    match unsafe {
        setsockopt(
            socket,
            SOL_SOCKET,
            SO_LINGER,
            &val as *const _ as *const c_char,
            size_of::<linger>() as c_int,
        )
    } {
        SOCKET_ERROR => Err(io::Error::last_os_error()),
        _ => Ok(()),
    }
}

pub(crate) fn get_linger(socket: TcpSocket) -> io::Result<Option<Duration>> {
    let mut val: linger = unsafe { std::mem::zeroed() };
    let mut len = size_of::<linger>() as c_int;

    match unsafe {
        getsockopt(
            socket,
            SOL_SOCKET,
            SO_LINGER,
            &mut val as *mut _ as *mut _,
            &mut len,
        )
    } {
        SOCKET_ERROR => Err(io::Error::last_os_error()),
        _ => {
            if val.l_onoff == 0 {
                Ok(None)
            } else {
                Ok(Some(Duration::from_secs(val.l_linger as u64)))
            }
        }
    }
}

pub(crate) fn set_recv_buffer_size(socket: TcpSocket, size: u32) -> io::Result<()> {
    let size = size.try_into().ok().unwrap_or_else(i32::max_value);
    match unsafe {
        setsockopt(
            socket,
            SOL_SOCKET,
            SO_RCVBUF,
            &size as *const _ as *const c_char,
            size_of::<c_int>() as c_int,
        )
    } {
        SOCKET_ERROR => Err(io::Error::last_os_error()),
        _ => Ok(()),
    }
}

pub(crate) fn get_recv_buffer_size(socket: TcpSocket) -> io::Result<u32> {
    let mut optval: c_int = 0;
    let mut optlen = size_of::<c_int>() as c_int;
    match unsafe {
        getsockopt(
            socket,
            SOL_SOCKET,
            SO_RCVBUF,
            &mut optval as *mut _ as *mut _,
            &mut optlen as *mut _,
        )
    } {
        SOCKET_ERROR => Err(io::Error::last_os_error()),
        _ => Ok(optval as u32),
    }
}

pub(crate) fn set_send_buffer_size(socket: TcpSocket, size: u32) -> io::Result<()> {
    let size = size.try_into().ok().unwrap_or_else(i32::max_value);
    match unsafe {
        setsockopt(
            socket,
            SOL_SOCKET,
            SO_SNDBUF,
            &size as *const _ as *const c_char,
            size_of::<c_int>() as c_int,
        )
    } {
        SOCKET_ERROR => Err(io::Error::last_os_error()),
        _ => Ok(()),
    }
}

pub(crate) fn get_send_buffer_size(socket: TcpSocket) -> io::Result<u32> {
    let mut optval: c_int = 0;
    let mut optlen = size_of::<c_int>() as c_int;
    match unsafe {
        getsockopt(
            socket,
            SOL_SOCKET,
            SO_SNDBUF,
            &mut optval as *mut _ as *mut _,
            &mut optlen as *mut _,
        )
    } {
        SOCKET_ERROR => Err(io::Error::last_os_error()),
        _ => Ok(optval as u32),
    }
}

pub(crate) fn set_keepalive(socket: TcpSocket, keepalive: bool) -> io::Result<()> {
    let val: BOOL = if keepalive { TRUE } else { FALSE };
    match unsafe {
        setsockopt(
            socket,
            SOL_SOCKET,
            SO_KEEPALIVE,
            &val as *const _ as *const c_char,
            size_of::<BOOL>() as c_int,
        )
    } {
        SOCKET_ERROR => Err(io::Error::last_os_error()),
        _ => Ok(()),
    }
}

pub(crate) fn get_keepalive(socket: TcpSocket) -> io::Result<bool> {
    let mut optval: c_char = 0;
    let mut optlen = size_of::<BOOL>() as c_int;

    match unsafe {
        getsockopt(
            socket,
            SOL_SOCKET,
            SO_KEEPALIVE,
            &mut optval as *mut _ as *mut _,
            &mut optlen,
        )
    } {
        SOCKET_ERROR => Err(io::Error::last_os_error()),
        _ => Ok(optval != FALSE as c_char),
    }
}

pub(crate) fn set_keepalive_params(socket: TcpSocket, keepalive: TcpKeepalive) -> io::Result<()> {
    /// Windows configures keepalive time/interval in a u32 of milliseconds.
    fn dur_to_ulong_ms(dur: Duration) -> c_ulong {
        dur.as_millis()
            .try_into()
            .ok()
            .unwrap_or_else(u32::max_value)
    }

    // If any of the fields on the `tcp_keepalive` struct were not provided by
    // the user, just leaving them zero will clobber any existing value.
    // Unfortunately, we can't access the current value, so we will use the
    // defaults if a value for the time or interval was not not provided.
    let time = keepalive.time.unwrap_or_else(|| {
        // The default value is two hours, as per
        // https://docs.microsoft.com/en-us/windows/win32/winsock/sio-keepalive-vals
        let two_hours = 2 * 60 * 60;
        Duration::from_secs(two_hours)
    });

    let interval = keepalive.interval.unwrap_or_else(|| {
        // The default value is one second, as per
        // https://docs.microsoft.com/en-us/windows/win32/winsock/sio-keepalive-vals
        Duration::from_secs(1)
    });

    let mut keepalive = mstcpip::tcp_keepalive {
        // Enable keepalive
        onoff: 1,
        keepalivetime: dur_to_ulong_ms(time),
        keepaliveinterval: dur_to_ulong_ms(interval),
    };

    let mut out = 0;
    match unsafe {
        WSAIoctl(
            socket,
            mstcpip::SIO_KEEPALIVE_VALS,
            &mut keepalive as *mut _ as LPVOID,
            size_of::<mstcpip::tcp_keepalive>() as DWORD,
            ptr::null_mut() as LPVOID,
            0 as DWORD,
            &mut out as *mut _ as LPDWORD,
            0 as LPWSAOVERLAPPED,
            None,
        )
    } {
        0 => Ok(()),
        _ => Err(io::Error::last_os_error()),
    }
}
