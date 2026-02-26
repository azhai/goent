package utils

import (
	"log/slog"
	"os"
	"path/filepath"
)

// CreateLogger create logger by log file
// If log file directory not exist, create it
func CreateLogger(logFile string) (logger *slog.Logger, err error) {
	var fh *os.File
	switch logFile {
	case "":
		return
	case "/dev/null":
		logger = slog.New(slog.DiscardHandler)
		return
	case "stdout":
		fh = os.Stdout
	case "stderr":
		fh = os.Stderr
	default:
		fh, err = CreateFileHandler(logFile)
		if err != nil {
			return
		}
	}
	opts := &slog.HandlerOptions{AddSource: false}
	logger = slog.New(slog.NewJSONHandler(fh, opts))
	return
}

func CreateFileHandler(logFile string) (*os.File, error) {
	if err := MakeDirForFile(logFile); err != nil {
		return nil, err
	}
	mode := os.O_RDWR | os.O_CREATE | os.O_APPEND
	return os.OpenFile(logFile, mode, 0666)
}

// MakeDirForFile create directory for file if not exist
func MakeDirForFile(filename string) (err error) {
	dir := filepath.Dir(filename)
	if _, err = os.Stat(dir); os.IsNotExist(err) {
		return os.MkdirAll(dir, 0755)
	}
	return err
}
