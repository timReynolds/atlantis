package events

import (
	"os"
	"path/filepath"
	"testing"

	. "github.com/runatlantis/atlantis/testing"
)

func TestPullDirSizeBytes(t *testing.T) {
	t.Run("returns total size for nested files", func(t *testing.T) {
		dir := t.TempDir()
		fileA := filepath.Join(dir, "a.txt")
		fileB := filepath.Join(dir, "nested", "b.txt")

		Ok(t, os.WriteFile(fileA, []byte("abc"), 0600))
		Ok(t, os.MkdirAll(filepath.Dir(fileB), 0700))
		Ok(t, os.WriteFile(fileB, []byte("12345"), 0600))

		size, err := pullDirSizeBytes(dir)
		Ok(t, err)
		Equals(t, int64(8), size)
	})

	t.Run("returns error for missing directory", func(t *testing.T) {
		missingDir := filepath.Join(t.TempDir(), "missing")
		_, err := pullDirSizeBytes(missingDir)
		Assert(t, err != nil, "expected error for missing dir")
		Equals(t, true, os.IsNotExist(err))
	})
}
