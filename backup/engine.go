package backup

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/azhai/goent"
)

// Engine creates and restores database backups.
type Engine struct {
	db  *goent.DB
	cfg Config
}

// New creates a backup engine for db with the provided configuration.
func New(db *goent.DB, cfg Config) *Engine {
	return &Engine{db: db, cfg: cfg}
}

// Full creates a full backup archive and returns its metadata.
//
// For SQLite this copies the database file. For PostgreSQL it dumps the schema
// and exports each non-log table with COPY.
func (e *Engine) Full(ctx context.Context, name string) Result {
	return e.create(ctx, "full", nil)
}

// Incremental creates an incremental backup archive containing rows changed
// since the provided base time. The caller is responsible for choosing the
// correct base time, typically the started time of the last successful full
// backup.
func (e *Engine) Incremental(ctx context.Context, name string, since time.Time) Result {
	return e.create(ctx, "incremental", &since)
}

// Restore extracts and applies the backup archive at path.
//
// If incremental is true, the engine expects a full backup archive to have been
// restored first; it will then apply the incremental changes on top.
func (e *Engine) Restore(ctx context.Context, path string, incremental bool) error {
	return e.extract(ctx, path, incremental)
}

func (e *Engine) create(ctx context.Context, backupType string, since *time.Time) Result {
	startedAt := time.Now().UTC()
	res := Result{StartedAt: &startedAt}

	if err := os.MkdirAll(e.cfg.Dir, 0755); err != nil {
		res.Err = err
		return res
	}

	timestamp := time.Now().UTC().Format("20060102_150405")
	filename := fmt.Sprintf("%s_%s.tar.gz", backupType, timestamp)
	path := filepath.Join(e.cfg.Dir, filename)

	file, err := os.Create(path)
	if err != nil {
		res.Err = err
		return res
	}
	defer file.Close()

	gw := gzip.NewWriter(file)
	defer gw.Close()
	tw := tar.NewWriter(gw)
	defer tw.Close()

	var size int64
	driverName := strings.ToLower(e.db.DriverName())

	switch driverName {
	case "postgres", "postgresql":
		size, err = e.createPostgresArchive(ctx, tw, backupType, since)
	default:
		size, err = e.createSQLiteArchive(ctx, tw)
	}

	if err != nil {
		res.Err = err
		return res
	}

	completedAt := time.Now().UTC()
	res.Path = path
	res.Size = size
	res.CompletedAt = &completedAt
	return res
}

func (e *Engine) extract(ctx context.Context, path string, incremental bool) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	gr, err := gzip.NewReader(file)
	if err != nil {
		return err
	}
	defer gr.Close()

	driverName := strings.ToLower(e.db.DriverName())
	switch driverName {
	case "postgres", "postgresql":
		return e.extractPostgresArchive(ctx, gr, incremental)
	default:
		return e.extractSQLiteArchive(ctx, gr)
	}
}

func writeFileToTar(tw *tar.Writer, name string, r io.Reader, size int64) error {
	header := &tar.Header{
		Name:    name,
		Mode:    0644,
		Size:    size,
		ModTime: time.Now().UTC(),
	}
	if err := tw.WriteHeader(header); err != nil {
		return err
	}
	_, err := io.Copy(tw, r)
	return err
}

func addFileToTar(tw *tar.Writer, filePath, headerName string) (int64, error) {
	fi, err := os.Stat(filePath)
	if err != nil {
		return 0, err
	}
	header, err := tar.FileInfoHeader(fi, "")
	if err != nil {
		return 0, err
	}
	header.Name = headerName
	if err := tw.WriteHeader(header); err != nil {
		return 0, err
	}
	f, err := os.Open(filePath)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	return io.Copy(tw, f)
}

func addDirToTar(tw *tar.Writer, dir, archivePrefix string) (int64, error) {
	var total int64
	err := filepath.Walk(dir, func(filePath string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		rel, err := filepath.Rel(dir, filePath)
		if err != nil {
			return err
		}
		headerName := rel
		if archivePrefix != "" {
			headerName = filepath.Join(archivePrefix, rel)
		}
		n, err := addFileToTar(tw, filePath, headerName)
		total += n
		return err
	})
	return total, err
}
