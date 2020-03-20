package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/jessevdk/go-flags"
	"github.com/pkg/xattr"
)

type Entry struct {
	Path   string
	Xattrs map[string][]byte
}

/*
	TODO
	Set exit code if any mismatches or errors
*/

func main() {
	var opts struct {
		Verbose bool `short:"v" long:"verbose" description:"Show verbose debug information"`
	}
	args, err := flags.Parse(&opts)
	if err != nil {
		notifyError(err)
		os.Exit(1)
	}

	if len(args) < 2 {
		notifyErrorString("must provide two directories to compare")
		os.Exit(1)
	}
	srcDir := args[0]
	destDir := args[1]

	if opts.Verbose {
		fmt.Printf("comparing %s to %s\n", srcDir, destDir)
	}

	chanBufferSize := 1000
	srcChan := make(chan *Entry, chanBufferSize)
	destChan := make(chan *Entry, chanBufferSize)

	var wg sync.WaitGroup
	wg.Add(3)

	var srcError error
	go func() {
		srcError = scanDirectory(srcDir, srcChan)
		wg.Done()
	}()

	var destError error
	go func() {
		destError = scanDirectory(destDir, destChan)
		wg.Done()
	}()

	var compareError error
	go func() {
		compareError = compareEntries(srcChan, destChan, srcDir, destDir, opts.Verbose)
		wg.Done()
	}()

	wg.Wait()

	if srcError != nil {
		notifyError(srcError)
	}
	if destError != nil {
		notifyError(destError)
	}
	if compareError != nil {
		notifyError(compareError)
	}
}

func notifyError(err error) {
	fmt.Fprintf(os.Stderr, "error: %s\n", err)
}

func notifyErrorString(err string) {
	fmt.Fprintf(os.Stderr, "error: %s\n", err)
}

func scanDirectory(dir string, entries chan<- *Entry) error {
	walkErr := filepath.Walk(dir, func(entryPath string, info os.FileInfo, err error) error {
		if err != nil {
			notifyError(err)
			return nil
		}

		if !info.Mode().IsRegular() {
			return nil
		}

		xattrs := make(map[string][]byte)
		xattrNames, err := xattr.List(entryPath)
		if err != nil {
			notifyError(err)
		} else {
			for _, name := range xattrNames {
				data, err := xattr.Get(entryPath, name)
				if err != nil {
					notifyError(err)
				}
				xattrs[name] = data
			}
		}

		entries <- &Entry{
			Path:   entryPath,
			Xattrs: xattrs,
		}

		return nil
	})

	close(entries)
	return walkErr
}

func compareEntries(src, dest <-chan *Entry, srcDir, destDir string, verbose bool) error {
	waitingForSrc := true
	waitingForDest := true
	srcComplete := false
	destComplete := false
	srcCount := 0
	destCount := 0
	var srcEntry *Entry
	var destEntry *Entry
	var more bool
	var lastStatusUpdate time.Time
	for {
		if waitingForSrc {
			select {
			case srcEntry, more = <-src:
				if more {
					waitingForSrc = false
				} else {
					waitingForSrc = false
					srcComplete = true
				}
			default:
			}
		} else if waitingForDest {
			select {
			case destEntry, more = <-dest:
				if more {
					waitingForDest = false
				} else {
					waitingForDest = false
					destComplete = true
				}
			default:
			}
		} else {
			// we have entries (or completion) from each side, time to do comparison
			if srcComplete && destComplete {
				break
			}
			if srcComplete {
				// all remaining dest entries are missing in src
				printOnlyIn(destDir, destEntry.Path)
				destCount++
				waitingForDest = true
			} else if destComplete {
				// all remaining src entries are missing in dest
				printOnlyIn(srcDir, srcEntry.Path)
				srcCount++
				waitingForSrc = true
			} else {
				// comparison - use relative paths because root dir doesn't matter
				if verbose && time.Since(lastStatusUpdate).Seconds() > 5 {
					fmt.Fprintf(os.Stderr, "src: %d processed, %d/%d queued, dest: %d processed, %d/%d queued\n", srcCount, len(src), cap(src), destCount, len(dest), cap(dest))
					lastStatusUpdate = time.Now()
				}
				srcPath, err := filepath.Rel(srcDir, srcEntry.Path)
				if err != nil {
					return err
				}
				destPath, err := filepath.Rel(destDir, destEntry.Path)
				if err != nil {
					return err
				}
				if srcPath < destPath {
					printOnlyIn(srcDir, srcEntry.Path)
					srcCount++
					waitingForSrc = true
				} else if srcPath > destPath {
					printOnlyIn(destDir, destEntry.Path)
					destCount++
					waitingForDest = true
				} else {
					// paths are equal, let's compare xattrs!
					compareXattrs(srcEntry, destEntry, srcPath, destPath)
					srcCount++
					destCount++
					waitingForSrc = true
					waitingForDest = true
				}
			}
		}
	}
	return nil
}

func printOnlyIn(dir, path string) {
	relPath, err := filepath.Rel(dir, path)
	if err != nil {
		relPath = path
	}
	fmt.Printf("only in %s: %s\n", dir, relPath)
}

func compareXattrs(srcEntry, destEntry *Entry, srcRelPath, destRelPath string) {
	// copy for safe modification
	destXattrs := make(map[string][]byte)
	for key, val := range destEntry.Xattrs {
		destXattrs[key] = val
	}

	for key, val := range srcEntry.Xattrs {
		destVal, ok := destXattrs[key]
		if ok {
			if !bytes.Equal(val, destVal) {
				printXattrMismatch(srcRelPath, destRelPath, key)
			}
			delete(destXattrs, key)
		} else {
			printXattrOnlyIn(srcEntry.Path, key)
		}
	}

	// iterate on remaining xattrs which must only exist in dest
	for key, _ := range destXattrs {
		printXattrOnlyIn(destEntry.Path, key)
	}
}

func printXattrOnlyIn(path, name string) {
	fmt.Printf("xattr only in %s: %s\n", path, name)
}

func printXattrMismatch(srcPath, destPath, name string) {
	fmt.Printf("%s %s differ: %s\n", srcPath, destPath, name)
}
