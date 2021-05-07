use crate::event::Source;
use crate::poll::{self, Registry};
use crate::sys::windows::buffer_pool::BufferPool;
use crate::sys::windows::lazycell::AtomicLazyCell;
use crate::sys::windows::{Event, PollOpt, ReadinessQueue, Ready, Registration, SetReadiness};
use crate::{Interest, Token};
use log::trace;
use miow;
use miow::iocp::{CompletionPort, CompletionStatus};
use std::cell::UnsafeCell;
use std::os::windows::prelude::*;
#[cfg(debug_assertions)]
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::{fmt, io};
use winapi::shared::winerror::WAIT_TIMEOUT;
use winapi::um::minwinbase::OVERLAPPED;
use winapi::um::minwinbase::OVERLAPPED_ENTRY;

const WAKE: Token = Token(std::usize::MAX);

/// Each Selector has a globally unique(ish) ID associated with it. This ID
/// gets tracked by `TcpStream`, `TcpListener`, etc... when they are first
/// registered with the `Selector`. If a type that is previously associated with
/// a `Selector` attempts to register itself with a different `Selector`, the
/// operation will return with an error. This matches windows behavior.
#[cfg(debug_assertions)]
static NEXT_ID: AtomicUsize = AtomicUsize::new(0);

/// The guts of the Windows event loop, this is the struct which actually owns
/// a completion port.
///
/// Internally this is just an `Arc`, and this allows handing out references to
/// the internals to I/O handles registered on this selector. This is
/// required to schedule I/O operations independently of being inside the event
/// loop (e.g. when a call to `write` is seen we're not "in the event loop").
#[derive(Debug)]
pub struct Selector {
    pub(super) inner: Arc<SelectorInner>,

    // Custom readiness queue
    pub(super) readiness_queue: ReadinessQueue,

    #[cfg(debug_assertions)]
    has_waker: AtomicBool,
}

// Public to allow `Waker` access
#[derive(Debug)]
pub(super) struct SelectorInner {
    /// Unique identifier of the `Selector`
    #[cfg(debug_assertions)]
    id: usize,

    /// The actual completion port that's used to manage all I/O
    port: CompletionPort,

    /// A pool of buffers usable by this selector.
    ///
    /// Primitives will take buffers from this pool to perform I/O operations,
    /// and once complete they'll be put back in.
    buffers: Mutex<BufferPool>,
}

impl Selector {
    pub fn new() -> io::Result<Selector> {
        // offset by 1 to avoid choosing 0 as the id of a selector
        #[cfg(debug_assertions)]
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed) + 1;
        let cp = CompletionPort::new(1)?;

        let inner = Arc::new(SelectorInner {
            #[cfg(debug_assertions)]
            id: id,
            port: cp,
            buffers: Mutex::new(BufferPool::new(256)),
        });

        let readiness_queue = ReadinessQueue::new(inner.clone())?;

        Ok(Selector {
            inner,
            readiness_queue,
            #[cfg(debug_assertions)]
            has_waker: AtomicBool::new(false),
        })
    }

    pub fn try_clone(&self) -> io::Result<Self> {
        Ok(Self {
            inner: self.inner.clone(),
            // TODO: ?
            readiness_queue: self.readiness_queue.clone(),
            #[cfg(debug_assertions)]
            has_waker: AtomicBool::new(self.has_waker.load(Ordering::Acquire)),
        })
    }

    #[cfg(debug_assertions)]
    pub fn register_waker(&self) -> bool {
        self.has_waker.swap(true, Ordering::AcqRel)
    }

    pub fn select(&self, events: &mut Events, mut timeout: Option<Duration>) -> io::Result<()> {
        // Compute the timeout value passed to the system selector. If the
        // readiness queue has pending nodes, we still want to poll the system
        // selector for new events, but we don't want to block the thread to
        // wait for new events.
        if timeout == Some(Duration::from_millis(0)) {
            // If blocking is not requested, then there is no need to prepare
            // the queue for sleep
            //
            // The sleep_marker should be removed by readiness_queue.poll().
        } else if self.readiness_queue.prepare_for_sleep() {
            // The readiness queue is empty. The call to `prepare_for_sleep`
            // inserts `sleep_marker` into the queue. This signals to any
            // threads setting readiness that the `Poll::poll` is going to
            // sleep, so the waker should be used.
        } else {
            // The readiness queue is not empty, so do not block the thread.
            timeout = Some(Duration::from_millis(0));
        }

        let ret = self.select2(events, timeout)?;

        // Poll custom event queue
        self.readiness_queue.poll(events);

        // Return number of polled events
        Ok(ret)
    }

    pub fn select2(&self, events: &mut Events, timeout: Option<Duration>) -> io::Result<()> {
        trace!("select; timeout={:?}", timeout);

        // Clear out the previous list of I/O events and get some more!
        events.clear();

        trace!("polling IOCP");
        let n = match self.inner.port.get_many(&mut events.statuses, timeout) {
            Ok(statuses) => statuses.len(),
            Err(ref e) if e.raw_os_error() == Some(WAIT_TIMEOUT as i32) => 0,
            Err(e) => return Err(e),
        };

        for status in events.statuses[..n].iter() {
            // This should only ever happen from the waker.
            if status.overlapped() as usize == 0 {
                let token = Token(status.token());
                if token == WAKE {
                    continue;
                }
                events.events.push(Event::new(Ready::READABLE, token));
                continue;
            }

            let callback = unsafe { (*(status.overlapped() as *mut Overlapped)).callback };

            trace!("select; -> got overlapped");
            callback(status.entry());
        }

        trace!("returning");
        Ok(())
    }

    /// Gets a new reference to this selector, although all underlying data
    /// structures will refer to the same completion port.
    pub(super) fn clone_inner(&self) -> Arc<SelectorInner> {
        self.inner.clone()
    }

    /// Return the `Selector`'s identifier
    #[cfg(debug_assertions)]
    pub fn id(&self) -> usize {
        self.inner.id
    }
}

impl SelectorInner {
    fn identical(&self, other: &SelectorInner) -> bool {
        (self as *const SelectorInner) == (other as *const SelectorInner)
    }

    pub fn port(&self) -> &CompletionPort {
        &self.port
    }
}

// A registration is stored in each I/O object which keeps track of how it is
// associated with a `Selector` above.
//
// Once associated with a `Selector`, a registration can never be un-associated
// (due to IOCP requirements). This is actually implemented through the
// `poll::Registration` and `poll::SetReadiness` APIs to keep track of all the
// level/edge/filtering business.
/// A `Binding` is embedded in all I/O objects associated with a `Poll`
/// object.
///
/// Each registration keeps track of which selector the I/O object is
/// associated with, ensuring that implementations of `Evented` can be
/// conformant for the various methods on Windows.
///
/// If you're working with custom IOCP-enabled objects then you'll want to
/// ensure that one of these instances is stored in your object and used in the
/// implementation of `Evented`.
///
/// For more information about how to use this see the `windows` module
/// documentation in this crate.
pub struct Binding {
    selector: AtomicLazyCell<Arc<SelectorInner>>,
}

impl Binding {
    /// Creates a new blank binding ready to be inserted into an I/O
    /// object.
    ///
    /// Won't actually do anything until associated with a `Poll` loop.
    pub fn new() -> Binding {
        Binding {
            selector: AtomicLazyCell::new(),
        }
    }

    /// Same as `register_handle` but for sockets.
    pub unsafe fn register_socket(
        &self,
        handle: &dyn AsRawSocket,
        token: Token,
        registry: &Registry,
    ) -> io::Result<()> {
        let selector = poll::selector(registry);
        drop(self.selector.fill(selector.inner.clone()));
        self.check_same_selector(registry)?;
        selector.inner.port.add_socket(usize::from(token), handle)
    }

    /// Same as `reregister_handle`, but for sockets.
    pub unsafe fn reregister_socket(
        &self,
        _socket: &dyn AsRawSocket,
        _token: Token,
        registry: &Registry,
    ) -> io::Result<()> {
        self.check_same_selector(registry)
    }

    /// Same as `deregister_handle`, but for sockets.
    pub unsafe fn deregister_socket(
        &self,
        _socket: &dyn AsRawSocket,
        registry: &Registry,
    ) -> io::Result<()> {
        self.check_same_selector(registry)
    }

    fn check_same_selector(&self, registry: &Registry) -> io::Result<()> {
        let selector = poll::selector(registry);
        match self.selector.borrow() {
            Some(prev) if prev.identical(&selector.inner) => Ok(()),
            Some(_) | None => Err(other("socket already registered")),
        }
    }
}

impl fmt::Debug for Binding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Binding").finish()
    }
}

/// Helper struct used for TCP and UDP which bundles a `binding` with a
/// `SetReadiness` handle.
pub struct ReadyBinding {
    binding: Binding,
    readiness: Option<SetReadiness>,
}

impl ReadyBinding {
    /// Creates a new blank binding ready to be inserted into an I/O object.
    ///
    /// Won't actually do anything until associated with an `Selector` loop.
    pub fn new() -> ReadyBinding {
        ReadyBinding {
            binding: Binding::new(),
            readiness: None,
        }
    }

    /// Returns whether this binding has been associated with a selector
    /// yet.
    pub fn registered(&self) -> bool {
        self.readiness.is_some()
    }

    /// Acquires a buffer with at least `size` capacity.
    ///
    /// If associated with a selector, this will attempt to pull a buffer from
    /// that buffer pool. If not associated with a selector, this will allocate
    /// a fresh buffer.
    pub fn get_buffer(&self, size: usize) -> Vec<u8> {
        match self.binding.selector.borrow() {
            Some(i) => i.buffers.lock().unwrap().get(size),
            None => Vec::with_capacity(size),
        }
    }

    /// Returns a buffer to this binding.
    ///
    /// If associated with a selector, this will push the buffer back into the
    /// selector's pool of buffers. Otherwise this will just drop the buffer.
    pub fn put_buffer(&self, buf: Vec<u8>) {
        if let Some(i) = self.binding.selector.borrow() {
            i.buffers.lock().unwrap().put(buf);
        }
    }

    /// Sets the readiness of this I/O object to a particular `set`.
    ///
    /// This is later used to fill out and respond to requests to `poll`. Note
    /// that this is all implemented through the `SetReadiness` structure in the
    /// `poll` module.
    pub fn set_readiness(&self, set: Ready) {
        if let Some(ref i) = self.readiness {
            trace!("set readiness to {:?}", set);
            i.set_readiness(set).expect("event loop disappeared?");
        }
    }

    /// Queries what the current readiness of this I/O object is.
    ///
    /// This is what's being used to generate events returned by `poll`.
    pub fn readiness(&self) -> Ready {
        match self.readiness {
            Some(ref i) => i.readiness(),
            None => Ready::EMPTY,
        }
    }

    /// Implementation of the `Evented::register` function essentially.
    ///
    /// Returns an error if we're already registered with another event loop,
    /// and otherwise just reassociates ourselves with the event loop to
    /// possible change tokens.
    pub(crate) fn register_socket(
        &mut self,
        socket: &dyn AsRawSocket,
        registry: &Registry,
        token: Token,
        events: Interest,
        registration: &Mutex<Option<Registration>>,
    ) -> io::Result<()> {
        trace!("register {:?} {:?}", token, events);
        unsafe {
            self.binding.register_socket(socket, token, registry)?;
        }

        let (r, s) = Registration::new(
            &poll::selector(registry).readiness_queue,
            token,
            events,
            PollOpt::edge(),
        );
        self.readiness = Some(s);
        *registration.lock().unwrap() = Some(r);
        Ok(())
    }

    /// Implementation of `Evented::reregister` function.
    pub(crate) fn reregister_socket(
        &mut self,
        socket: &dyn AsRawSocket,
        registry: &Registry,
        token: Token,
        events: Interest,
        registration: &Mutex<Option<Registration>>,
    ) -> io::Result<()> {
        trace!("reregister {:?} {:?}", token, events);
        unsafe {
            self.binding.reregister_socket(socket, token, registry)?;
        }

        registration
            .lock()
            .unwrap()
            .as_mut()
            .unwrap()
            .reregister(registry, token, events)
    }

    /// Implementation of the `Evented::deregister` function.
    ///
    /// Doesn't allow registration with another event loop, just shuts down
    /// readiness notifications and such.
    pub(crate) fn deregister(
        &mut self,
        socket: &dyn AsRawSocket,
        registry: &Registry,
        registration: &Mutex<Option<Registration>>,
    ) -> io::Result<()> {
        trace!("deregistering");
        unsafe {
            self.binding.deregister_socket(socket, registry)?;
        }

        registration
            .lock()
            .unwrap()
            .as_mut()
            .unwrap()
            .deregister(registry)
    }
}

fn other(s: &str) -> io::Error {
    io::Error::new(io::ErrorKind::Other, s)
}

#[derive(Debug)]
pub struct Events {
    /// Raw I/O event completions are filled in here by the call to `get_many`
    /// on the completion port above. These are then processed to run callbacks
    /// which figure out what to do after the event is done.
    statuses: Box<[CompletionStatus]>,

    /// Literal events returned by `get` to the upwards `EventLoop`. This file
    /// doesn't really modify this (except for the waker), instead almost all
    /// events are filled in by the `ReadinessQueue` from the `poll` module.
    events: Vec<Event>,
}

impl Events {
    pub fn with_capacity(cap: usize) -> Events {
        // Note that it's possible for the output `events` to grow beyond the
        // capacity as it can also include deferred events, but that's certainly
        // not the end of the world!
        Events {
            statuses: vec![CompletionStatus::zero(); cap].into_boxed_slice(),
            events: Vec::with_capacity(cap),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    pub fn len(&self) -> usize {
        self.events.len()
    }

    pub fn capacity(&self) -> usize {
        self.events.capacity()
    }

    pub fn get(&self, idx: usize) -> Option<&Event> {
        self.events.get(idx)
    }

    pub fn push_event(&mut self, event: Event) {
        self.events.push(event);
    }

    pub fn clear(&mut self) {
        self.events.truncate(0);
    }
}

macro_rules! overlapped2arc {
    ($e:expr, $t:ty, $($field:ident).+) => ({
        let offset = offset_of!($t, $($field).+);
        debug_assert!(offset < mem::size_of::<$t>());
        FromRawArc::from_raw(($e as usize - offset) as *mut $t)
    })
}

macro_rules! offset_of {
    ($t:ty, $($field:ident).+) => (
        &(*(0 as *const $t)).$($field).+ as *const _ as usize
    )
}
#[repr(C)]
pub(crate) struct Overlapped {
    inner: UnsafeCell<miow::Overlapped>,
    pub(crate) callback: fn(&OVERLAPPED_ENTRY),
}

#[cfg(feature = "os-ext")]
impl Overlapped {
    pub(crate) fn new(cb: fn(&OVERLAPPED_ENTRY)) -> Overlapped {
        Overlapped {
            inner: UnsafeCell::new(miow::Overlapped::zero()),
            callback: cb,
        }
    }

    pub(crate) fn as_ptr(&self) -> *const OVERLAPPED {
        unsafe { (*self.inner.get()).raw() }
    }
    pub(crate) fn as_mut_ptr(&self) -> *mut OVERLAPPED {
        unsafe { (*self.inner.get()).raw() }
    }
}

impl fmt::Debug for Overlapped {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Overlapped").finish()
    }
}

unsafe impl Send for Overlapped {}
unsafe impl Sync for Overlapped {}
