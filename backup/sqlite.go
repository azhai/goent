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
)

// sqliteDBPath extracts the filesystem path from a SQLite DSN.
// It returns an empty string when the database is not a file-backed SQLite DB.
func sqliteDBPath(dsn string) string {
	if strings.Contains(dsn, "mode=memory") {
		return ""
	}
	dsn = strings.TrimPrefix(dsn, "file:")
	if idx := strings.Index(dsn, "?"); idx >= 0 {
		dsn = dsn[:idx]
	}
	dsn = strings.TrimSpace(dsn)
	if dsn == "" || dsn == ":memory:" {
		return ""
	}
	return dsn
}

func (e *Engine) createSQLiteArchive(ctx context.Context, tw *tar.Writer) (int64, error) {
	dbPath := e.cfg.DBPath
	if dbPath == "" {
		dbPath = sqliteDBPath(e.cfg.DSN)
	}
	if dbPath == "" {
		return 0, fmt.Errorf("sqlite backup requires a file-backed database, set Config.DBPath or Config.DSN")
	}

	fi, err := os.Stat(dbPath)
	if err != nil {
		return 0, err
	}

	header, err := tar.FileInfoHeader(fi, "")
	if err != nil {
		return 0, err
	}
	header.Name = "db" + filepath.Ext(dbPath)
	if err := tw.WriteHeader(header); err != nil {
		return 0, err
	}

	f, err := os.Open(dbPath)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	return io.Copy(tw, f)
}

func (e *Engine) extractSQLiteArchive(ctx context.Context, gr *gzip.Reader) error {
	dbPath := e.cfg.DBPath
	if dbPath == "" {
		dbPath = sqliteDBPath(e.cfg.DSN)
	}
	if dbPath == "" {
		return fmt.Errorf("sqlite restore requires a file-backed database, set Config.DBPath or Config.DSN")
	}

	tr := tar.NewReader(gr)
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if header.Typeflag != tar.TypeReg || !strings.HasPrefix(header.Name, "db") {
			continue
		}
		if err := os.MkdirAll(filepath.Dir(dbPath), 0755); err != nil {
			return err
		}
		out, err := os.Create(dbPath)
		if err != nil {
			return err
		}
		if _, err := io.Copy(out, tr); err != nil {
			out.Close()
			return err
		}
		return out.Close()
	}
	return fmt.Errorf("no database file found in backup archive")
}
