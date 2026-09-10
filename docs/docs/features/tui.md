# Interactive mode

Running `marina` without a command opens an interactive explorer over your local cache and every configured registry:

~~~bash
marina
~~~

## Screens

| Key | Screen | Contents |
| --- | --- | --- |
| `1` | datasets | Every dataset merged across the cache and the registries, grouped by namespace and by base name the way `marina ls --remote` prints it. The `L R` column says where each one is. `L` for the local cache, `R` for a registry, a dim dot for neither.  `SIZE` is the uncompressed size, cached or remote. `l` narrows the list to one side or the other, and the panel title says which. The pane on the right carries the rest: registries, compression, hashes, and its files, read straight from the cache directory |
| `2` | registries | Configured registries, their listing state, and the default marker |
| `3` | settings | Time display, default registry, timeouts, and the compression defaults |

Press `?` at any time for the full key map.
