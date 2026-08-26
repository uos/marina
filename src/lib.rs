//! Marina's stable public facade.
//!
//! The implementation lives in `mt_dataset` so Minot and Marina can share the
//! dataset stack without either application depending on the other.

pub use mt_dataset::*;

// Re-exporting Rust items does not by itself expose their C symbols from this
// crate's cdylib, so keep Marina's existing C ABI as a thin forwarding layer.
#[cfg(not(test))]
mod ffi_exports {
    use std::ffi::{c_char, c_void};

    use mt_dataset::ffi::{MarinaProgressCallback, MarinaResolveDetailed};

    #[unsafe(no_mangle)]
    unsafe extern "C" fn marina_resolve_detailed(
        target: *const c_char,
        registry: *const c_char,
    ) -> MarinaResolveDetailed {
        unsafe { mt_dataset::ffi::marina_resolve_detailed(target, registry) }
    }

    #[unsafe(no_mangle)]
    unsafe extern "C" fn marina_free_resolve_detailed(result: *mut MarinaResolveDetailed) {
        unsafe { mt_dataset::ffi::marina_free_resolve_detailed(result) }
    }

    #[unsafe(no_mangle)]
    unsafe extern "C" fn marina_resolve(
        target: *const c_char,
        registry: *const c_char,
    ) -> *mut c_char {
        unsafe { mt_dataset::ffi::marina_resolve(target, registry) }
    }

    #[unsafe(no_mangle)]
    extern "C" fn marina_pull(bag_ref: *const c_char, registry: *const c_char) -> *mut c_char {
        mt_dataset::ffi::marina_pull(bag_ref, registry)
    }

    #[unsafe(no_mangle)]
    extern "C" fn marina_pull_with_progress(
        bag_ref: *const c_char,
        registry: *const c_char,
        progress_mode: i32,
    ) -> *mut c_char {
        mt_dataset::ffi::marina_pull_with_progress(bag_ref, registry, progress_mode)
    }

    #[unsafe(no_mangle)]
    extern "C" fn marina_pull_with_callback(
        bag_ref: *const c_char,
        registry: *const c_char,
        callback: MarinaProgressCallback,
        user_data: *mut c_void,
    ) -> *mut c_char {
        mt_dataset::ffi::marina_pull_with_callback(bag_ref, registry, callback, user_data)
    }

    #[unsafe(no_mangle)]
    extern "C" fn marina_last_error_message() -> *mut c_char {
        mt_dataset::ffi::marina_last_error_message()
    }

    #[unsafe(no_mangle)]
    unsafe extern "C" fn marina_free_string(ptr: *mut c_char) {
        unsafe { mt_dataset::ffi::marina_free_string(ptr) }
    }
}
