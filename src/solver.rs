//! The browser solver: `browser::Browser::solve` drives a real headed Chrome on a virtual
//! framebuffer to clear an interactive bot-protection challenge and lift the resulting cookies
//! into a [`crate::clearance::Clearance`].
//!
//! **Firewall (must hold):** this module and its submodules have zero references to the egress
//! pool, leases, the worker, the store, or the `Client` — the browser cannot see an exit. Its
//! only outward seam is [`observe::Observer`]. This was a crate boundary before the workspace was
//! collapsed into one crate; keep it a one-way dependency by convention.

pub mod browser;
pub mod capture;
pub mod challenge;
pub mod click;
pub mod display;
pub mod frame;
pub mod locate;
pub mod observe;
pub mod session;
pub mod settle;
