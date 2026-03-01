import marina

r = marina.resolve_detailed("tag")
print("resolve kind:", r.kind, "should_pull:", r.should_pull)
if r.kind in ("local", "cached"):
    print("path:", r.path)
elif r.kind == "remote_available":
    print("remote bag:", r.bag, "registry:", r.registry)
    print("pulling with progress...")
    print("pulled:", marina.pull_with_progress(r.bag, r.registry, progress=True))
else:
    print("resolve error:", r.message)
