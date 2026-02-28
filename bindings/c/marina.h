#ifndef MARINA_H
#define MARINA_H

#ifdef __cplusplus
extern "C" {
#endif

// Resolve status values.
enum {
  MARINA_RESOLVE_ERROR = -1,
  MARINA_RESOLVE_LOCAL = 0,
  MARINA_RESOLVE_CACHED = 1,
  MARINA_RESOLVE_REMOTE_AVAILABLE = 2
};

typedef struct MarinaResolveDetailed {
  int kind;
  char *path;      // set when kind is LOCAL or CACHED
  char *bag;       // set when kind is REMOTE_AVAILABLE
  char *registry;  // set when kind is REMOTE_AVAILABLE
  char *message;   // optional human-readable message
} MarinaResolveDetailed;

// Detailed resolve response for C callers.
MarinaResolveDetailed marina_resolve_detailed(const char *target);

// Frees string fields inside MarinaResolveDetailed and clears pointers.
void marina_free_resolve_detailed(MarinaResolveDetailed *result);

// Returns allocated UTF-8 string (caller frees with marina_free_string) or NULL on error.
// Legacy helper: returns local/cached path, or REMOTE:<bag>@<registry> marker.
char *marina_resolve(const char *target);

// Returns local path after pull (caller frees with marina_free_string) or NULL on error.
// Pass NULL for registry to use default selection.
char *marina_pull(const char *bag_ref, const char *registry);

// Returns last error message (allocated string, caller frees) or NULL if none.
char *marina_last_error_message(void);

// Frees any string returned by this library.
void marina_free_string(char *ptr);

#ifdef __cplusplus
}
#endif

#endif
