# C / C++

The `marina` shared library exposes a C API through `marina.h`. It can be used from C, C++, or any language with a C FFI.

## Get the header and library

The header is at `bindings/c/marina.h` in the source tree. The shared library (`libmarina.so` / `libmarina.dylib`) is the `cdylib` build artefact of the `marina` crate.

Build the library:

~~~bash
cargo build --release
# outputs: target/release/libmarina.so (Linux) or libmarina.dylib (macOS)
~~~

## Resolve a dataset

`marina_resolve_detailed` returns a `MarinaResolveDetailed` struct describing where the dataset is. Always free it with `marina_free_resolve_detailed` when done.

~~~c
#include "marina.h"
#include <stdio.h>

int main(void) {
    MarinaResolveDetailed r = marina_resolve_detailed("outdoor-run:v2", NULL);

    if (r.kind == MARINA_RESOLVE_LOCAL || r.kind == MARINA_RESOLVE_CACHED) {
        printf("ready at: %s\n", r.path);
    } else if (r.kind == MARINA_RESOLVE_REMOTE_AVAILABLE) {
        printf("remote: bag=%s registry=%s\n", r.bag, r.registry);
    } else if (r.kind == MARINA_RESOLVE_AMBIGUOUS) {
        printf("ambiguous: first match bag=%s registry=%s\n", r.bag, r.registry);
    } else {
        fprintf(stderr, "error: %s\n", r.message ? r.message : "unknown");
    }

    marina_free_resolve_detailed(&r);
    return 0;
}
~~~

Pass a registry name as the second argument to restrict the search:

~~~c
MarinaResolveDetailed r = marina_resolve_detailed("outdoor-run:v2", "team-ssh");
~~~

### `MarinaResolveDetailed` fields

| Field | Type | Description |
|---|---|---|
| `kind` | `int` | One of the `MARINA_RESOLVE_*` constants |
| `path` | `char *` | Local path; set when `kind` is `LOCAL` or `CACHED` |
| `bag` | `char *` | Dataset reference; set when `kind` is `REMOTE_AVAILABLE` or `AMBIGUOUS` |
| `registry` | `char *` | Registry name; set when `kind` is `REMOTE_AVAILABLE` or `AMBIGUOUS` |
| `message` | `char *` | Human-readable status or error message |

### `kind` constants

| Constant | Value | Meaning |
|---|---|---|
| `MARINA_RESOLVE_ERROR` | `-1` | Resolution failed |
| `MARINA_RESOLVE_LOCAL` | `0` | Target is an existing local path |
| `MARINA_RESOLVE_CACHED` | `1` | Target is in the Marina cache |
| `MARINA_RESOLVE_REMOTE_AVAILABLE` | `2` | Target exists in a remote registry |
| `MARINA_RESOLVE_AMBIGUOUS` | `3` | Target found in multiple registries; first match returned |

## Pull a dataset

`marina_pull` downloads the dataset and returns the local cache path. The caller frees the returned string with `marina_free_string`.

~~~c
char *path = marina_pull("outdoor-run:v2", NULL);
if (!path) {
    char *err = marina_last_error_message();
    fprintf(stderr, "pull failed: %s\n", err ? err : "unknown");
    marina_free_string(err);
    return 1;
}
printf("cached at: %s\n", path);
marina_free_string(path);
~~~

## Pull with progress output

`marina_pull_with_progress` accepts a progress mode flag:

~~~c
// Print progress events to stdout
char *path = marina_pull_with_progress("outdoor-run:v2", NULL, MARINA_PROGRESS_MODE_STDOUT);
~~~

| Constant | Value | Meaning |
|---|---|---|
| `MARINA_PROGRESS_MODE_SILENT` | `0` | No output |
| `MARINA_PROGRESS_MODE_STDOUT` | `1` | Write progress lines to stdout |

## Pull with a custom progress callback

`marina_pull_with_callback` calls a function pointer for every progress event:

~~~c
#include "marina.h"
#include <stdio.h>

static void on_progress(const char *phase, const char *message, void *user_data) {
    (void)user_data;
    printf("[%-8s] %s\n", phase, message);
}

int main(void) {
    MarinaResolveDetailed r = marina_resolve_detailed("outdoor-run:v2", NULL);
    if (r.kind != MARINA_RESOLVE_REMOTE_AVAILABLE) {
        marina_free_resolve_detailed(&r);
        return 1;
    }

    char *path = marina_pull_with_callback(r.bag, r.registry, on_progress, NULL);
    if (!path) {
        char *err = marina_last_error_message();
        fprintf(stderr, "pull failed: %s\n", err ? err : "unknown");
        marina_free_string(err);
        marina_free_resolve_detailed(&r);
        return 1;
    }

    printf("done: %s\n", path);
    marina_free_string(path);
    marina_free_resolve_detailed(&r);
    return 0;
}
~~~

The callback signature is:

~~~c
typedef void (*MarinaProgressCallback)(
    const char *phase,      // short phase name, e.g. "download", "unpack"
    const char *message,    // human-readable description
    void *user_data         // pointer passed through from the call site
);
~~~

## Memory management

All strings returned by the library are heap-allocated. Free them with `marina_free_string`. Never pass a string obtained from one library call directly to another function that also returns a string — always free them explicitly.

~~~c
void marina_free_string(char *ptr);
void marina_free_resolve_detailed(MarinaResolveDetailed *result);
~~~

## Error handling

On failure, string-returning functions return `NULL`. Retrieve the last error with `marina_last_error_message` and free it with `marina_free_string`:

~~~c
char *err = marina_last_error_message(); // NULL if no error
if (err) {
    fprintf(stderr, "marina error: %s\n", err);
    marina_free_string(err);
}
~~~

## C++ usage

The header includes `extern "C"` guards, so you can `#include "marina.h"` directly in C++ translation units.

## Compile and link

~~~bash
gcc example.c -o example \
    -L./target/release \
    -lmarina \
    -Wl,-rpath,./target/release
~~~
