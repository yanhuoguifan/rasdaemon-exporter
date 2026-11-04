package collector_test

import (
	"archive/tar"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestDistPackagesStaticBinaryAndReleaseFiles(t *testing.T) {
	make, err := exec.LookPath("make")
	if err != nil {
		t.Skip("GNU Make is required to execute the dist recipe")
	}
	for _, tool := range []string{"tar", "sha256sum"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("%s is required by the dist recipe", tool)
		}
	}

	root, err := os.MkdirTemp("", "codex-dist-test-")
	if err != nil {
		t.Fatalf("create isolated dist test directory: %v", err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(root); err != nil {
			t.Errorf("remove isolated dist test directory: %v", err)
		}
	})
	project := filepath.Join(root, "project")
	if err := os.MkdirAll(project, 0o755); err != nil {
		t.Fatalf("create isolated project directory: %v", err)
	}
	for _, name := range []string{"Makefile", "rasdaemon-exporter.service", "LICENSE", "README.md", "METRICS.md"} {
		copyDistInput(t, filepath.Join(moduleRoot(t), name), filepath.Join(project, name))
	}
	if err := os.Mkdir(filepath.Join(project, "build"), 0o755); err != nil {
		t.Fatalf("create isolated build directory: %v", err)
	}
	distDir := filepath.Join(project, "dist")
	if err := os.Mkdir(distDir, 0o755); err != nil {
		t.Fatalf("create isolated dist directory: %v", err)
	}
	unrelatedPath := filepath.Join(distDir, "keep.txt")
	if err := os.WriteFile(unrelatedPath, []byte("unrelated file"), 0o644); err != nil {
		t.Fatalf("write unrelated dist file: %v", err)
	}
	staleArchiveName := "rasdaemon-exporter-0.0.0-linux-amd64.tar.gz"
	staleArchivePath := filepath.Join(distDir, staleArchiveName)
	if err := os.WriteFile(staleArchivePath, []byte("previous release archive"), 0o644); err != nil {
		t.Fatalf("write previous release archive: %v", err)
	}
	version := makefileVersion(t, filepath.Join(project, "Makefile"))
	distName := "rasdaemon-exporter-" + version + "-linux-amd64"
	fixture := filepath.Join(root, "fixture-binary")
	if err := os.WriteFile(fixture, []byte("#!/bin/sh\nprintf 'test binary\\n'\n"), 0o755); err != nil {
		t.Fatalf("write fixture binary: %v", err)
	}
	argsLog := filepath.Join(root, "go-args")
	goWrapper := filepath.Join(root, "go-wrapper")
	wrapper := "#!/bin/sh\nprintf 'CGO_ENABLED=%s CC=%s GOOS=%s GOARCH=%s\\n' \"$CGO_ENABLED\" \"$CC\" \"$GOOS\" \"$GOARCH\" > \"$DIST_TEST_ARGS\"\n" +
		"printf '%s\\n' \"$@\" >> \"$DIST_TEST_ARGS\"\n" +
		"output=\nprevious=\nfor arg do\n  if [ \"$previous\" = -o ]; then output=$arg; break; fi\n  previous=$arg\ndone\n" +
		"test -n \"$output\" && cp \"$DIST_TEST_BINARY\" \"$output\" && chmod 0755 \"$output\"\n"
	if err := os.WriteFile(goWrapper, []byte(wrapper), 0o755); err != nil {
		t.Fatalf("write fixture Go wrapper: %v", err)
	}
	for run := 0; run < 2; run++ {
		command := exec.Command(make, "--no-print-directory", "dist", "GO="+goWrapper)
		command.Dir = project
		command.Env = append(os.Environ(), "DIST_TEST_BINARY="+fixture, "DIST_TEST_ARGS="+argsLog)
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("make dist run %d: %v\n%s", run+1, err, output)
		}
	}
	if got, err := os.ReadFile(unrelatedPath); err != nil || string(got) != "unrelated file" {
		t.Fatalf("unrelated dist file = (%q, %v), want unchanged", got, err)
	}
	if got, err := os.ReadFile(staleArchivePath); err != nil || string(got) != "previous release archive" {
		t.Fatalf("previous release archive = (%q, %v), want unchanged", got, err)
	}
	args, err := os.ReadFile(argsLog)
	if err != nil {
		t.Fatalf("read build arguments: %v", err)
	}
	for _, expected := range []string{
		"CGO_ENABLED=1 CC=musl-gcc GOOS=linux GOARCH=amd64",
		"netgo osusergo sqlite_omit_load_extension",
		"-linkmode=external -extldflags=-static -X main.version=" + version,
	} {
		if !strings.Contains(string(args), expected) {
			t.Errorf("static build args do not contain %q:\n%s", expected, args)
		}
	}

	archiveName := distName + ".tar.gz"
	archivePath := filepath.Join(project, "dist", archiveName)
	wantMembers := []string{
		distName + "/LICENSE",
		distName + "/METRICS.md",
		distName + "/README.md",
		distName + "/rasdaemon-exporter",
		distName + "/rasdaemon-exporter.service",
	}
	if got := distMembers(t, archivePath); !slices.Equal(got, wantMembers) {
		t.Fatalf("archive members = %v, want %v", got, wantMembers)
	}
	binary := extractDistBinary(t, archivePath, filepath.Join(root, "extract"), distName)
	if contents, err := os.ReadFile(binary); err != nil || string(contents) != "#!/bin/sh\nprintf 'test binary\\n'\n" {
		t.Fatalf("archive binary = (%q, %v), want fixture binary", contents, err)
	}
	if info, err := os.Stat(binary); err != nil || info.Mode().Perm()&0o111 == 0 {
		t.Fatalf("archive binary is not executable: info=%v err=%v", info, err)
	}

	archiveData, err := os.ReadFile(archivePath)
	if err != nil {
		t.Fatalf("read release archive: %v", err)
	}
	digest := sha256.Sum256(archiveData)
	wantSum := hex.EncodeToString(digest[:]) + "  " + archiveName + "\n"
	checksumName := "SHA256SUMS-" + version
	checksumPath := filepath.Join(project, "dist", checksumName)
	if got, err := os.ReadFile(checksumPath); err != nil || string(got) != wantSum {
		t.Fatalf("%s = (%q, %v), want %q", checksumName, got, err, wantSum)
	}
	verify := exec.Command("sha256sum", "-c", checksumName)
	verify.Dir = filepath.Dir(checksumPath)
	if output, err := verify.CombinedOutput(); err != nil {
		t.Fatalf("verify release checksum: %v\n%s", err, output)
	}
}

func copyDistInput(t *testing.T, source, destination string) {
	t.Helper()
	contents, err := os.ReadFile(source)
	if err != nil {
		t.Fatalf("read dist input %s: %v", source, err)
	}
	if err := os.WriteFile(destination, contents, 0o644); err != nil {
		t.Fatalf("copy dist input %s: %v", source, err)
	}
}

func makefileVersion(t *testing.T, path string) string {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read Makefile: %v", err)
	}
	for _, line := range strings.Split(string(contents), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 3 && fields[0] == "VERSION" && fields[1] == "=" {
			return fields[2]
		}
	}
	t.Fatal("Makefile has no literal VERSION assignment")
	return ""
}

func distMembers(t *testing.T, path string) []string {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open release archive: %v", err)
	}
	defer file.Close()
	compressed, err := gzip.NewReader(file)
	if err != nil {
		t.Fatalf("open release gzip: %v", err)
	}
	defer compressed.Close()
	reader := tar.NewReader(compressed)
	var members []string
	for {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read tar entry: %v", err)
		}
		if header.Typeflag != tar.TypeDir {
			members = append(members, header.Name)
		}
	}
	slices.Sort(members)
	return members
}

func extractDistBinary(t *testing.T, archive, destination, prefix string) string {
	t.Helper()
	file, err := os.Open(archive)
	if err != nil {
		t.Fatalf("open release archive: %v", err)
	}
	defer file.Close()
	compressed, err := gzip.NewReader(file)
	if err != nil {
		t.Fatalf("open release gzip: %v", err)
	}
	defer compressed.Close()
	if err := os.Mkdir(destination, 0o755); err != nil {
		t.Fatalf("create extraction directory: %v", err)
	}
	reader := tar.NewReader(compressed)
	for {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read tar entry: %v", err)
		}
		if header.Name != prefix+"/rasdaemon-exporter" {
			continue
		}
		path := filepath.Join(destination, "rasdaemon-exporter")
		contents, err := io.ReadAll(reader)
		if err != nil {
			t.Fatalf("read archive binary: %v", err)
		}
		if err := os.WriteFile(path, contents, os.FileMode(header.Mode).Perm()); err != nil {
			t.Fatalf("extract archive binary: %v", err)
		}
		return path
	}
	t.Fatal("release archive does not contain the exporter binary")
	return ""
}
