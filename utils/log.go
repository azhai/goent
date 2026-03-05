package utils

import (
	"bytes"
	"fmt"
	"go/format"
	"log/slog"
	"os"
	"path/filepath"
)

// CreateLogger creates a logger by log file
// If log file directory doesn't exist, it creates it
// Returns the created logger or an error
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

// CreateFileHandler creates a file handler for logging
// It ensures the directory exists and opens the file for writing
// Returns the file handler or an error
func CreateFileHandler(logFile string) (*os.File, error) {
	if err := MakeDirForFile(logFile); err != nil {
		return nil, err
	}
	mode := os.O_RDWR | os.O_CREATE | os.O_APPEND
	return os.OpenFile(logFile, mode, 0666)
}

// MakeDirForFile creates directory for file if it doesn't exist
// It creates all necessary parent directories
// Returns an error if directory creation fails
func MakeDirForFile(filename string) (err error) {
	dir := filepath.Dir(filename)
	if _, err = os.Stat(dir); os.IsNotExist(err) {
		return os.MkdirAll(dir, 0755)
	}
	return err
}

// WriteToFile writes the formatted content to a file.
func WriteToFile(buf *bytes.Buffer, outputPath string) error {
	formatted, err := format.Source(buf.Bytes())
	if err != nil {
		return fmt.Errorf("error formatting output: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(outputPath), 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}
	if err := os.WriteFile(outputPath, formatted, 0644); err != nil {
		return fmt.Errorf("error writing file: %w", err)
	}
	return nil
}
