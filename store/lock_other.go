//go:build !(darwin || dragonfly || freebsd || linux || netbsd || openbsd)

package store

import "os"

// lockFile does nothing where flock is not available. The journal is then as
// unguarded as it was before the lock existed: two processes on one journal
// are the operator's to prevent. Refusing to open instead would stop the
// bridge running at all on those platforms, for a protection that matters
// only when it is misconfigured.
func lockFile(string) (*os.File, error) {
	return nil, nil
}
