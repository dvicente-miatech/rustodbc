//! `BlockingEngine`: fachada sincrona en Rust (no un wrapper de
//! `asyncio.run`). Fase 11 -- deliberadamente despues de que la API async
//! este estable, para que sea una proyeccion delgada y no una segunda
//! implementacion.
//!
//! - Runtime tokio propio (current-thread + pool de blocking), `block_on`
//!   con el GIL liberado (`Python::allow_threads`).
//! - Pensada para los `~50` call-sites del consumidor que hoy envuelven
//!   `ISeriesConnection` en `asyncio.to_thread(...)` desde codigo sincrono
//!   (crons de arq, parsers): mismo patron de migracion de menor a mayor
//!   riesgo descripto en AGENTS.md ss "migracion del consumidor", paso 1.
//! - Levanta `InterfaceError` si se llama desde un hilo con un event loop
//!   *corriendo* -- para que nadie termine bloqueando el loop de arq.
