//! `SQLGetDiagRecW` -> `crate::errors::Diagnostic`. Unico punto de contacto
//! con la cadena de diagnosticos ODBC; todo lo demas en `core::ffi` llama
//! aca cuando un `SqlReturn` no es `SUCCESS`/`SUCCESS_WITH_INFO`.

use odbc_sys::{Handle, HandleType, SQLGetDiagRecW, SqlReturn};

use crate::errors::Diagnostic;

use super::wchar::from_utf16_lossy;

/// Tamano de buffer para el mensaje de texto de un registro de diagnostico.
/// ODBC no tiene un limite duro documentado; 1024 unidades UTF-16 cubre
/// cualquier mensaje de SQLCODE real observado en DB2 for i con margen.
const MESSAGE_BUFFER_LEN: usize = 1024;

/// Junta TODA la cadena de diagnosticos asociada a un handle (no solo el
/// primer registro) -- `SQLGetDiagRecW` puede devolver varios registros para
/// un mismo error, y descartar los siguientes pierde contexto real (p.ej.
/// una violacion de constraint que trae detalle en el segundo registro).
pub fn collect_diagnostics(handle_type: HandleType, handle: Handle) -> Vec<Diagnostic> {
    let mut diagnostics = Vec::new();
    let mut rec_number: i16 = 1;

    loop {
        let mut sqlstate = [0u16; 6]; // 5 caracteres + margen
        let mut native_error: i32 = 0;
        let mut message = [0u16; MESSAGE_BUFFER_LEN];
        let mut text_length: i16 = 0;

        let ret = unsafe {
            SQLGetDiagRecW(
                handle_type,
                handle,
                rec_number,
                sqlstate.as_mut_ptr(),
                &mut native_error,
                message.as_mut_ptr(),
                message.len() as i16,
                &mut text_length,
            )
        };

        match ret {
            SqlReturn::SUCCESS | SqlReturn::SUCCESS_WITH_INFO => {
                // sqlstate viene null-terminado por el driver; recortamos en
                // el primer \0 antes de decodificar.
                let sqlstate_len = sqlstate.iter().position(|&c| c == 0).unwrap_or(5);
                let sqlstate_str = from_utf16_lossy(&sqlstate[..sqlstate_len]);

                let msg_len = (text_length.max(0) as usize).min(message.len());
                let message_str = from_utf16_lossy(&message[..msg_len]);

                diagnostics.push(Diagnostic {
                    sqlstate: sqlstate_str,
                    native_code: native_error,
                    message: message_str,
                });

                rec_number += 1;
            }
            _ => break,
        }
    }

    diagnostics
}

/// El diagnostico "principal" para reportar como error de Rust: el primero
/// de la cadena, o un placeholder generico si el driver no dejo ninguno
/// (pasa con algunos drivers en ciertos codigos de error).
pub fn primary_diagnostic(handle_type: HandleType, handle: Handle) -> Diagnostic {
    collect_diagnostics(handle_type, handle)
        .into_iter()
        .next()
        .unwrap_or_else(|| Diagnostic {
            sqlstate: "HY000".to_string(),
            native_code: 0,
            message: "el driver ODBC no dejo diagnostico para este error".to_string(),
        })
}
