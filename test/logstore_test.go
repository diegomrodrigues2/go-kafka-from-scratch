package test

import (
	"path/filepath"
	"testing"

	"github.com/diegomrodrigues2/go-kafka-from-scratch/internal/logstore"
)

func TestAppendRead(t *testing.T) {
	dir := t.TempDir()
	lg, err := logstore.Open(filepath.Join(dir, "log"), 1<<20)
	if err != nil {
		t.Fatal(err)
	}

	var offs []uint64
	for i := 0; i < 10; i++ {
		off, err := lg.Append([]byte{byte(i)})
		if err != nil {
			t.Fatal(err)
		}
		offs = append(offs, off)
	}

	recs, last, err := lg.ReadFrom(offs[0], 1024)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 10 || last != offs[9] {
		t.Fatalf("got %d recs last=%d", len(recs), last)
	}

	if err := lg.Close(); err != nil {
		t.Fatal(err)
	}

	lg2, err := logstore.Open(filepath.Join(dir, "log"), 1<<20)
	if err != nil {
		t.Fatal(err)
	}

	off, err := lg2.Append([]byte("X"))
	if err != nil {
		t.Fatal(err)
	}
	if off != offs[9]+1 {
		t.Fatalf("offset continuity broken, got=%d", off)
	}
}
