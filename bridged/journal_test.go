package bridged

import (
	"bytes"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestARepairedJournalIsLogged: a journal whose tail was torn by a crash is
// repaired at open, and the daemon says so. Nothing reported durable was lost,
// but the last process died writing, and an operator needs to know that.
func TestARepairedJournalIsLogged(t *testing.T) {
	path := filepath.Join(t.TempDir(), "swaps.journal")
	if err := os.WriteFile(path, []byte(`0000 {"hash":`), 0o600); err != nil {
		t.Fatal(err)
	}

	var out bytes.Buffer
	log := slog.New(slog.NewTextHandler(&out, nil))

	j, err := loadJournal(path, log)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()

	if !j.Repaired {
		t.Fatal("the test's journal was not torn")
	}
	if !strings.Contains(out.String(), "level=WARN") ||
		!strings.Contains(out.String(), "torn record") {

		t.Errorf("the repair was not logged as a warning: %q", out.String())
	}

	// And a journal that needed nothing says nothing.
	out.Reset()
	j.Close()
	again, err := loadJournal(path, log)
	if err != nil {
		t.Fatal(err)
	}
	defer again.Close()
	if out.Len() != 0 {
		t.Errorf("an intact journal was logged: %q", out.String())
	}
}
