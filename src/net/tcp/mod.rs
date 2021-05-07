#[cfg(not(windows))]
mod listener;
#[cfg(not(windows))]
pub use self::listener::TcpListener;
#[cfg(windows)]
pub use crate::sys::TcpListener;

mod socket;
// #[cfg(not(windows))]
pub use self::socket::{TcpKeepalive, TcpSocket};
// #[cfg(windows)]
// pub use crate::sys::tcp::TcpSocket;

#[cfg(not(windows))]
mod stream;
#[cfg(not(windows))]
pub use self::stream::TcpStream;
#[cfg(windows)]
pub use crate::sys::TcpStream;
