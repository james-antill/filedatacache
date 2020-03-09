package filedatacache

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	roc "github.com/james-antill/rename-on-close"
)

// Metadata cached for a given file/keys
type Metadata map[string]string

// Key to a file, Full normalized Path along with the Size and ModTime ... if
// any of this changes then we don't get the cache data.
type Key struct {
	Path    string
	ModTime time.Time
	Size    int64
}

// FileMode is a wrapper around os.FileMode that implements .IsSymlink()
type FileMode struct{ os.FileMode }

// IsSymlink reports whether m describes a directory. That is, it tests for the ModeSymlink bit being set in m.
func (m FileMode) IsSymlink() bool {
	return (m.FileMode & os.ModeSymlink) != 0
}

func normPath(root string) (string, error) {
	root, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}

	fi, err := os.Lstat(root)
	if err != nil {
		return "", err
	}

	hfi := FileMode{fi.Mode()}
	if hfi.IsSymlink() {
		nr, err := filepath.EvalSymlinks(root)
		if err != nil {
			return "", err
		}
		return nr, nil
	}

	return root, nil
}

// Normalize the path in the Key
func (k *Key) Normalize() error {
	kpath, err := normPath(k.Path)
	if err != nil {
		return err
	}

	k.Path = kpath
	return nil
}

// CacheRoot returns the root of the cache, or the empty string
func CacheRoot() string {
	root := ""
	if d, err := os.UserCacheDir(); err == nil {
		root = d
	} else {
		return ""
	}

	path := root + "/filedatacache"

	return path
}

// FDC is the holder for a file data cache root.
type FDC struct{ root string }

// New get a new FDC holder, from the default CacheRoot()
func New() *FDC {
	d := CacheRoot()
	if d == "" {
		return nil
	}

	return &FDC{d}
}

// NewRoot get a new FDC holder, from the given root)
func NewRoot(root string) *FDC {
	return &FDC{root}
}

// KeyFromPath gets a full Key, with a normalized path, from any path
func KeyFromPath(path string) (Key, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return Key{}, fmt.Errorf("KeyFromPath %v: %w", path, err)
	}

	k := Key{path, fi.ModTime(), fi.Size()}
	if err := k.Normalize(); err != nil {
		return Key{}, fmt.Errorf("KeyFromPath %v: %w", path, err)
	}

	return k, nil
}

func atoi(s string) (int64, error) {
	i64, err := strconv.ParseInt(s, 10, 64)
	return i64, err
}

// Get the Metadata for a given file/key
func (fdc *FDC) Get(k Key) Metadata {
	p := fdc.root + "/path/" + k.Path

	fi, err := os.Stat(p)
	if err != nil {
		return nil
	}

	if fi.ModTime() != k.ModTime {
		return nil
	}

	fior, err := os.Open(p)
	if err != nil {
		return nil
	}
	defer fior.Close()

	// Does it have the correct header...
	scanner := bufio.NewScanner(fior)
	if !scanner.Scan() {
		return nil
	}
	switch scanner.Text() {
	case "filedatacache-1.0":
		break
	default:
		return nil
	}

	// Does it have the internal mtime, for cache validation...
	if !scanner.Scan() {
		return nil
	}
	mtmtxt := scanner.Text()
	if !strings.HasPrefix(mtmtxt, "mtime: ") {
		return nil
	}
	// FIXME: Load mtime and test it's the same as Stat() ?
	// Really need to this when we support not having Stat() mtime

	// Does it have the internal length, for cache validation...
	if !scanner.Scan() {
		return nil
	}
	lentxt := scanner.Text()
	if !strings.HasPrefix(lentxt, "size: ") {
		return nil
	}
	size, err := atoi(lentxt[6:])
	if err != nil {
		return nil
	}
	if size != k.Size {
		return nil
	}

	md := make(Metadata)
	for scanner.Scan() {
		txt := scanner.Text()
		kv := strings.SplitN(txt, ": ", 2)
		if len(kv) != 2 {
			return nil
		}
		md[kv[0]] = kv[1]
	}
	if err := scanner.Err(); err != nil {
		return nil
	}

	return md
}

// sortedStringKeys to iterate a map[string]string in sorted key order
func sortedStringKeys(d map[string]string) []string {
	ret := make([]string, 0, len(d))

	for v := range d {
		ret = append(ret, v)
	}

	sort.Strings(ret)

	return ret
}

// Put new Metadata in the Cache for a given file/key. Fails silently.
func (fdc *FDC) Put(k Key, md Metadata) error {
	p := fdc.root + "/path/" + k.Path

	if err := os.MkdirAll(filepath.Dir(p), 0755); err != nil {
		return fmt.Errorf("Put %v: %w", k.Path, err)
	}

	fiow, err := roc.Create(p)
	if err != nil {
		return fmt.Errorf("Put %v: %w", k.Path, err)
	}
	defer fiow.Close()

	iow := bufio.NewWriter(fiow)
	fmt.Fprintln(iow, "filedatacache-1.0")
	tm := k.ModTime
	if tm.Nanosecond() == 0 {
		fmt.Fprintf(iow, "mtime: %d\n", tm.Unix())
	} else {
		timeFmt := ".000000000"
		fmt.Fprintf(iow, "mtime: %d%s\n", tm.Unix(), tm.Format(timeFmt))
	}
	fmt.Fprintf(iow, "size: %d\n", k.Size)

	// Now save the metadata, in order because sameness...
	for _, k := range sortedStringKeys(md) {
		v := md[k]
		fmt.Fprintf(iow, "%s: %s\n", k, v)
	}

	if err := iow.Flush(); err != nil {
		return fmt.Errorf("Put %v: %w", k.Path, err)
	}
	if err := fiow.CloseRename(); err != nil {
		return fmt.Errorf("Put %v: %w", k.Path, err)
	}

	if err := os.Chtimes(p, time.Now(), k.ModTime); err != nil {
		return fmt.Errorf("Put %v: %w", k.Path, err)
	}

	return nil
}
