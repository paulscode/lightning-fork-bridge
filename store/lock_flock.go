//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package store

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

// lockFile takes an exclusive lock on path, creating it if need be, and
// returns the open file that holds it. Closing the file releases the lock, and
// so does the process ending, however it ends: a lock left by a crash is not
// one, so nothing has to clear it by hand.
//
// flock rather than fcntl's record locks: those belong to the process and are
// dropped when any descriptor on the file is closed, and a second Open of the
// same journal in one process would pass them.
func lockFile(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("opening the journal's lock file: %w", err)
	}
	err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, fmt.Errorf("%w: %s is held", ErrLocked, path)
		}

		return nil, fmt.Errorf("locking %s: %w", path, err)
	}

	return f, nil
}
