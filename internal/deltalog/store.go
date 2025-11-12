package deltalog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	deltaDirName           = "_delta_log"
	commitFileSuffix       = ".json"
	checkpointFileSuffix   = ".checkpoint.json"
	defaultCheckpointEvery = uint64(10)
	tempFilePrefix         = ".delta-tmp-"
	versionWidth           = 20
)

// Store implements a minimal Delta Lake style transaction log for a single
// topic-partition. It writes numbered JSON commits and optional checkpoints.
type Store struct {
	baseDir         string
	logDir          string
	checkpointEvery uint64
	prettyPrint     bool

	mu             sync.RWMutex
	nextVersion    uint64
	lastCheckpoint uint64
	hasCheckpoint  bool
}

// Option configures Store construction.
type Option func(*options)

type options struct {
	checkpointEvery uint64
	prettyPrint     bool
}

// WithCheckpointInterval customizes how often checkpoints should be generated.
func WithCheckpointInterval(every uint64) Option {
	return func(o *options) {
		if every == 0 {
			return
		}
		o.checkpointEvery = every
	}
}

// WithPrettyJSON toggles indented JSON for easier debugging.
func WithPrettyJSON(enable bool) Option {
	return func(o *options) {
		o.prettyPrint = enable
	}
}

func defaultOptions() options {
	return options{
		checkpointEvery: defaultCheckpointEvery,
	}
}

// Open prepares a Store bound to the provided partition directory. The caller
// must ensure exclusive ownership over the directory to avoid cross-process
// races when appending commits.
func Open(partitionDir string, opts ...Option) (*Store, error) {
	if partitionDir == "" {
		return nil, errors.New("partition directory required")
	}
	conf := defaultOptions()
	for _, opt := range opts {
		if opt != nil {
			opt(&conf)
		}
	}
	logDir := filepath.Join(partitionDir, deltaDirName)
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		return nil, err
	}
	st := &Store{
		baseDir:         partitionDir,
		logDir:          logDir,
		checkpointEvery: conf.checkpointEvery,
		prettyPrint:     conf.prettyPrint,
	}
	if err := st.reloadVersions(); err != nil {
		return nil, err
	}
	return st, nil
}

// Dir returns the fully qualified _delta_log directory path.
func (s *Store) Dir() string {
	return s.logDir
}

// NextVersion exposes the next commit version expected by the store. The
// caller should pass this value as the expectedVersion argument to Commit to
// implement optimistic concurrency control.
func (s *Store) NextVersion() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.nextVersion
}

// CurrentVersion reports the last written commit number, returning false when
// the log does not contain any entries yet.
func (s *Store) CurrentVersion() (uint64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.nextVersion == 0 {
		return 0, false
	}
	return s.nextVersion - 1, true
}

// CheckpointInterval returns the configured cadence to help coordination logic
// decide when to trigger a snapshot.
func (s *Store) CheckpointInterval() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.checkpointEvery
}

// Commit persists a transaction entry after ensuring the caller holds the
// expected next version. The supplied Commit pointer is populated with the
// version/timestamp that landed on disk.
func (s *Store) Commit(ctx context.Context, entry *Commit, expectedVersion uint64) (uint64, error) {
	if entry == nil {
		return 0, errors.New("commit entry required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.nextVersion != expectedVersion {
		return 0, ErrVersionConflict
	}

	version := expectedVersion
	entry.Version = version
	if entry.Timestamp.IsZero() {
		entry.Timestamp = time.Now().UTC()
	}

	targetPath := s.commitPath(version)
	if err := s.writeJSON(ctx, targetPath, entry); err != nil {
		return 0, err
	}

	s.nextVersion++
	return version, nil
}

// WriteCheckpoint stores the provided snapshot metadata tied to a specific
// commit version, ensuring the checkpoint cannot target a future version.
func (s *Store) WriteCheckpoint(ctx context.Context, ckpt *Checkpoint) error {
	if ckpt == nil {
		return errors.New("checkpoint required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if ckpt.Version >= s.nextVersion {
		return ErrCheckpointOutOfRange
	}
	if ckpt.Timestamp.IsZero() {
		ckpt.Timestamp = time.Now().UTC()
	}

	targetPath := s.checkpointPath(ckpt.Version)
	if err := s.writeJSON(ctx, targetPath, ckpt); err != nil {
		return err
	}

	s.lastCheckpoint = ckpt.Version
	s.hasCheckpoint = true
	return nil
}

// Replay loads the latest checkpoint (if any) plus every commit after it so
// higher layers can rebuild their state during recovery.
func (s *Store) Replay(ctx context.Context) (*Replay, error) {
	s.mu.RLock()
	nextVersion := s.nextVersion
	checkpointVersion := s.lastCheckpoint
	hasCheckpoint := s.hasCheckpoint
	s.mu.RUnlock()

	var ckpt *Checkpoint
	var start uint64
	if hasCheckpoint {
		reader, err := s.readCheckpoint(checkpointVersion)
		if err != nil {
			return nil, err
		}
		ckpt = reader
		start = checkpointVersion + 1
	}

	commits, err := s.readCommitRange(start, nextVersion)
	if err != nil {
		return nil, err
	}

	return &Replay{Checkpoint: ckpt, Commits: commits}, nil
}

// ApplyExternalCommit persists a commit produced by another node, ensuring the
// local version counter stays in sync.
func (s *Store) ApplyExternalCommit(ctx context.Context, commit *Commit) error {
	if commit == nil {
		return errors.New("commit required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if commit.Version < s.nextVersion {
		// already applied
		return nil
	}
	if commit.Version != s.nextVersion {
		return ErrVersionConflict
	}
	if ctx == nil {
		ctx = context.Background()
	}
	target := s.commitPath(commit.Version)
	if err := s.writeJSON(ctx, target, commit); err != nil {
		return err
	}
	s.nextVersion++
	return nil
}

// Commits returns all commit entries starting from the provided version.
func (s *Store) Commits(fromVersion uint64) ([]*Commit, error) {
	s.mu.RLock()
	to := s.nextVersion
	s.mu.RUnlock()
	if fromVersion >= to {
		return nil, nil
	}
	return s.readCommitRange(fromVersion, to)
}

// reloadVersions scans the _delta_log directory to determine the next commit
// version and most recent checkpoint.
func (s *Store) reloadVersions() error {
	entries, err := os.ReadDir(s.logDir)
	if err != nil {
		return err
	}
	var commitVersions []uint64
	var checkpointVersions []uint64

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasSuffix(name, checkpointFileSuffix) {
			versionStr := strings.TrimSuffix(name, checkpointFileSuffix)
			version, err := strconv.ParseUint(versionStr, 10, 64)
			if err != nil {
				continue
			}
			checkpointVersions = append(checkpointVersions, version)
			continue
		}
		if strings.HasSuffix(name, commitFileSuffix) && !strings.Contains(name, ".checkpoint") {
			versionStr := strings.TrimSuffix(name, commitFileSuffix)
			version, err := strconv.ParseUint(versionStr, 10, 64)
			if err != nil {
				continue
			}
			commitVersions = append(commitVersions, version)
		}
	}

	sort.Slice(commitVersions, func(i, j int) bool { return commitVersions[i] < commitVersions[j] })
	sort.Slice(checkpointVersions, func(i, j int) bool { return checkpointVersions[i] < checkpointVersions[j] })

	if len(commitVersions) > 0 {
		s.nextVersion = commitVersions[len(commitVersions)-1] + 1
	} else {
		s.nextVersion = 0
	}
	if len(checkpointVersions) > 0 {
		s.lastCheckpoint = checkpointVersions[len(checkpointVersions)-1]
		s.hasCheckpoint = true
	} else {
		s.lastCheckpoint = 0
		s.hasCheckpoint = false
	}
	return nil
}

func (s *Store) commitPath(version uint64) string {
	return filepath.Join(s.logDir, fmt.Sprintf("%0*d%s", versionWidth, version, commitFileSuffix))
}

func (s *Store) checkpointPath(version uint64) string {
	return filepath.Join(s.logDir, fmt.Sprintf("%0*d%s", versionWidth, version, checkpointFileSuffix))
}

func (s *Store) writeJSON(_ context.Context, path string, payload any) error {
	tmp, err := os.CreateTemp(s.logDir, tempFilePrefix)
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	enc := json.NewEncoder(tmp)
	enc.SetEscapeHTML(false)
	if s.prettyPrint {
		enc.SetIndent("", "  ")
	}
	if err := enc.Encode(payload); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	return nil
}

func (s *Store) readCheckpoint(version uint64) (*Checkpoint, error) {
	ckpt := &Checkpoint{}
	if err := s.readJSONFile(s.checkpointPath(version), ckpt); err != nil {
		return nil, err
	}
	return ckpt, nil
}

func (s *Store) readCommitRange(from, to uint64) ([]*Commit, error) {
	if from >= to {
		return nil, nil
	}
	commits := make([]*Commit, 0, to-from)
	for version := from; version < to; version++ {
		entry := &Commit{}
		if err := s.readJSONFile(s.commitPath(version), entry); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil, fmt.Errorf("%w: %d", ErrMissingVersion, version)
			}
			return nil, err
		}
		commits = append(commits, entry)
	}
	return commits, nil
}

func (s *Store) readJSONFile(path string, dest any) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	dec := json.NewDecoder(f)
	return dec.Decode(dest)
}
