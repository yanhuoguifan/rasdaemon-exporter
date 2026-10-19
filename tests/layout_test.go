package collector_test

import (
	"go/scanner"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSharedProductionFilesStayWithinCodeLineLimit(t *testing.T) {
	root := moduleRoot(t)
	files := []string{filepath.Join(root, "main.go")}
	collectorFiles, err := filepath.Glob(filepath.Join(root, "internal", "collector", "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range collectorFiles {
		name := filepath.Base(path)
		if strings.HasPrefix(name, "table_") {
			continue
		}
		files = append(files, path)
	}

	for _, path := range files {
		count, err := countCodeLines(path)
		if err != nil {
			t.Fatalf("count production code lines in %s: %v", path, err)
		}
		if count > 150 {
			t.Errorf("%s has %d non-comment, non-blank code lines; limit is 150", path, count)
		}
	}
}

func TestCountCodeLinesHandlesCommentsPunctuationAndRawStrings(t *testing.T) {
	path := filepath.Join(t.TempDir(), "line-count.go")
	source := "package sample\n" +
		"// A comment-only line does not count.\n" +
		"var value = 1 // Inline comments do not add a line.\n" +
		"\n" +
		"func example() {\n" +
		"\t;\n" +
		"\t_ = `first line\n" +
		"\t// Text inside the raw string is code data.\n" +
		"\tsecond line\n" +
		"\n" +
		"`\n" +
		"}\n"
	if err := os.WriteFile(path, []byte(source), 0o600); err != nil {
		t.Fatal(err)
	}

	got, err := countCodeLines(path)
	if err != nil {
		t.Fatalf("count fixture code lines: %v", err)
	}
	const want = 9
	if got != want {
		t.Fatalf("countCodeLines() = %d, want %d", got, want)
	}
}

// countCodeLines counts source lines touched by Go tokens. Comment tokens do
// not count, while every physical line inside a raw string literal does.
func countCodeLines(path string) (int, error) {
	source, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	fileSet := token.NewFileSet()
	file := fileSet.AddFile(path, -1, len(source))
	var sourceScanner scanner.Scanner
	sourceScanner.Init(file, source, nil, scanner.ScanComments)
	codeLines := make(map[int]struct{})
	physicalLines := strings.Split(string(source), "\n")

	for {
		position, kind, literal := sourceScanner.Scan()
		if kind == token.EOF {
			break
		}
		if kind == token.COMMENT {
			continue
		}
		if kind == token.SEMICOLON && literal == "\n" {
			continue
		}

		startOffset := file.Offset(position)
		tokenLength := len(literal)
		if tokenLength == 0 {
			// Punctuation tokens have an empty literal but occupy one byte.
			tokenLength = 1
		}
		lastOffset := startOffset + tokenLength - 1
		if lastOffset >= len(source) {
			lastOffset = len(source) - 1
		}
		startLine := file.Position(position).Line
		endLine := file.Position(file.Pos(lastOffset)).Line
		for line := startLine; line <= endLine; line++ {
			if line > len(physicalLines) || strings.TrimSpace(physicalLines[line-1]) == "" {
				continue
			}
			codeLines[line] = struct{}{}
		}
	}
	return len(codeLines), nil
}
