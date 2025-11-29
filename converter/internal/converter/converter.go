package converter

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
)

type FileSaver interface {
	Save(ctx context.Context, reader io.Reader, filename string, size int64) (int64, string, error)
}

type MockConverter struct {
	fileStore FileSaver
	baseDir   string

	sem chan struct{}
}

func NewMockConverter(fileStore FileSaver, baseDir string, maxParallel int) *MockConverter {
	if maxParallel <= 0 {
		maxParallel = 1
	}

	return &MockConverter{fileStore: fileStore, baseDir: baseDir, sem: make(chan struct{}, maxParallel)}
}

func (m *MockConverter) Convert(ctx context.Context, inputPath string, suggestedName string) (string, error) {
	select {
	case m.sem <- struct{}{}:
		defer func() { <-m.sem }()
	case <-ctx.Done():
		return "", fmt.Errorf("converter queue full or canceled: %w", ctx.Err())
	}

	delay := time.Duration(500+rand.Intn(1500)) * time.Millisecond
	select {
	case <-time.After(delay):
	case <-ctx.Done():
		return "", ctx.Err()
	}

	pdf := bytes.NewReader([]byte("%PDF-1.4\n% Mock DWG->PDF\n1 0 obj<<>>endobj\ntrailer<<>>\n%%EOF\n"))

	base := suggestedName
	if base == "" {
		base = filepath.Base(inputPath)
	}
	base = filepath.Base(base)
	name := strings.TrimSuffix(base, filepath.Ext(base))
	if name == "" {
		name = "output"
	}
	pdfName := uuid.NewString() + "_" + name + ".pdf"

	if _, _, err := m.fileStore.Save(ctx, pdf, pdfName, pdf.Size()); err != nil {
		return "", err
	}

	return pdfName, nil
}
