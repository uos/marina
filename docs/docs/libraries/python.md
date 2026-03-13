# Python

The `marina` Python package provides bindings for resolving and pulling datasets from within Python scripts or data pipelines.

## Install

~~~bash
uv add marina-py

# or with pip
pip install marina-py
~~~

A typical workflow can look something like this:

~~~python
import marina

r = marina.resolve_detailed("outdoor-run:v2")
if r.should_pull:
    path = marina.pull_with_progress(r.bag, r.registry, progress=True)
else:
    path = r.path

print("dataset ready at:", path)
~~~

## Resolve a dataset

`resolve_detailed` returns a `ResolveDetailed` object describing where the dataset is:

~~~python
import marina

r = marina.resolve_detailed("outdoor-run:v2")
print("kind:", r.kind)          # "local" | "cached" | "remote_available" | "ambiguous" | "error"
print("should pull:", r.should_pull)

if r.kind in ("local", "cached"):
    print("path:", r.path)
elif r.kind == "remote_available":
    print("bag:", r.bag, "registry:", r.registry)
else:
    print("error:", r.message)
~~~

`resolve` is the simple variant that returns the local path directly, or `"REMOTE:<bag>@<registry>"` for remote datasets:

~~~python
result = marina.resolve("outdoor-run:v2")
print(result)
~~~

Target a specific registry:

~~~python
r = marina.resolve_detailed("outdoor-run:v2", registry="team-ssh")
~~~

### `ResolveDetailed` fields

| Field | Type | Description |
|---|---|---|
| `kind` | `str` | `"local"`, `"cached"`, `"remote_available"`, `"ambiguous"`, or `"error"` |
| `path` | `str \| None` | Local path, set when `kind` is `"local"` or `"cached"` |
| `bag` | `str \| None` | Dataset reference, set when `kind` is `"remote_available"` or `"ambiguous"` |
| `registry` | `str \| None` | Registry name, set when `kind` is `"remote_available"` or `"ambiguous"` |
| `message` | `str \| None` | Human-readable status or error message |
| `should_pull` | `bool` | `True` when `kind` is `"remote_available"` or `"ambiguous"` |

## Pull a dataset

`pull` downloads a dataset if it is not already cached and returns the local path:

~~~python
path = marina.pull("outdoor-run:v2")
print("cached at:", path)
~~~

Pull from a specific registry:

~~~python
path = marina.pull("outdoor-run:v2", registry="team-ssh")
~~~

## Pull with progress output

`pull_with_progress` accepts a `progress` flag to print phase events to stdout, or a custom `writer` for redirecting output:

~~~python
# Print progress to stdout
path = marina.pull_with_progress("outdoor-run:v2", progress=True)

# Redirect progress to a custom writer
import sys
path = marina.pull_with_progress("outdoor-run:v2", writer=sys.stderr)
~~~

