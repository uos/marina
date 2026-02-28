import marina

r = marina.resolve_detailed("dlg_cut")
print("resolve kind:", r.kind, "should_pull:", r.should_pull)
if r.kind in ("local", "cached"):
    print("path:", r.path)
elif r.kind == "remote_available":
    print("remote bag:", r.bag, "registry:", r.registry)
    print("pulled:", marina.pull(r.bag, r.registry))
else:
    print("resolve error:", r.message)
