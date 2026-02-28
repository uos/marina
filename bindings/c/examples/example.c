#include <stdio.h>
#include "../marina.h"

int main(void) {
    MarinaResolveDetailed r = marina_resolve_detailed("dlg_cut");
    if (r.kind == MARINA_RESOLVE_LOCAL || r.kind == MARINA_RESOLVE_CACHED) {
        printf("ready locally: %s\n", r.path);
    } else if (r.kind == MARINA_RESOLVE_REMOTE_AVAILABLE) {
        printf("remote available: bag=%s registry=%s\n", r.bag, r.registry);
        printf("pulling now...\n");
        char *pulled = marina_pull(r.bag, r.registry);
        if (!pulled) {
            char *err = marina_last_error_message();
            fprintf(stderr, "pull failed: %s\n", err ? err : "unknown error");
            marina_free_string(err);
            marina_free_resolve_detailed(&r);
            return 1;
        }
        printf("pulled to: %s\n", pulled);
        marina_free_string(pulled);
    } else {
        fprintf(stderr, "resolve error: %s\n", r.message ? r.message : "unknown error");
        marina_free_resolve_detailed(&r);
        return 1;
    }
    marina_free_resolve_detailed(&r);
    return 0;
}
