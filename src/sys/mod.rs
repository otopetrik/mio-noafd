//! Module with system specific types.
//!
//! Required types:
//!
//! * `Event`: a type alias for the system specific event, e.g. `kevent` or
//!            `epoll_event`.
//! * `event`: a module with various helper functions for `Event`, see
//!            [`crate::event::Event`] for the required functions.
//! * `Events`: collection of `Event`s, see [`crate::Events`].
//! * `IoSourceState`: state for the `IoSource` type.
//! * `Selector`: selector used to register event sources and poll for events,
//!               see [`crate::Poll`] and [`crate::Registry`] for required
//!               methods.
//! * `tcp` and `udp` modules: see the [`crate::net`] module.
//! * `Waker`: see [`crate::Waker`].

#[cfg(unix)]
cfg_os_poll! {
    mod unix;
    pub use self::unix::*;
}

#[cfg(windows)]
cfg_os_poll! {
    mod windows;
    pub use self::windows::*;
}

cfg_not_os_poll! {
    mod shell;
    pub(crate) use self::shell::*;

    #[cfg(unix)]
    cfg_any_os_ext! {
        mod unix;
        pub use self::unix::SourceFd;
    }

    #[cfg(unix)]
    cfg_net! {
        pub use self::unix::SocketAddr;
    }
}
