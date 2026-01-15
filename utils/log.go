package utils

import (
	"log/slog"
	"os"
	"path/filepath"
)

// CreateLogger create logger by log file
// If log file directory not exist, create it
func CreateLogger(logFile string) (*slog.Logger, error) {
	if logFile == "/dev/null" {
		logger := slog.New(slog.DiscardHandler)
		return logger, nil
	} else if logFile == "stdout" {
		logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
		return logger, nil
	} else if logFile == "stderr" {
		logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
		return logger, nil
	}

	if err := MakeDirForFile(logFile); err != nil {
		return nil, err
	}
	mode := os.O_RDWR | os.O_CREATE | os.O_APPEND
	fh, err := os.OpenFile(logFile, mode, 0666)
	if err != nil {
		return nil, err
	}
	opts := &slog.HandlerOptions{AddSource: false}
	logger := slog.New(slog.NewJSONHandler(fh, opts))
	return logger, nil
}

// MakeDirForFile create directory for file if not exist
func MakeDirForFile(filename string) (err error) {
	dir := filepath.Dir(filename)
	if _, err = os.Stat(dir); os.IsNotExist(err) {
		return os.MkdirAll(dir, 0755)
	}
	return err
}
