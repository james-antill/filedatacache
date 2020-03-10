package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/james-antill/filedatacache"
)

func usage() {
	fmt.Fprintln(os.Stderr, "Usage: fdc <summary|get|put|add> file [key:value]...")
	os.Exit(2)
}

func sortedIntKeys(d map[int]int64) []int {
	ret := make([]int, 0, len(d))

	for v := range d {
		ret = append(ret, v)
	}

	sort.Ints(ret)

	return ret
}

func sortedInt64Keys(d map[int64]int64) []int64 {
	ret := make([]int64, 0, len(d))

	for v := range d {
		ret = append(ret, v)
	}

	sort.Slice(ret, func(i, j int) bool { return ret[i] < ret[j] })

	return ret
}

func main() {

	fdc := filedatacache.New()
	if fdc == nil {
		fmt.Fprintln(os.Stderr, "Can't find Cache.")
		os.Exit(1)
	}

	var k filedatacache.Key

	switch os.Args[1] {
	case "sum":
		fallthrough
	case "summary":
		if len(os.Args) < 2 {
			usage()
		}

	default:
		if len(os.Args) < 3 {
			usage()
		}
		k, _ = filedatacache.KeyFromPath(os.Args[2])
	}

	var md filedatacache.Metadata

	switch os.Args[1] {
	case "sum":
		fallthrough
	case "summary": // Also removes old entries ... eh.
		szData := make(map[int64]int64)
		numData := make(map[int]int64)
		err := filepath.Walk(fdc.CacheRoot(), func(path string, fi os.FileInfo, err error) error {
			if fi.IsDir() {
				return nil
			}

			rpath := strings.TrimPrefix(path, fdc.CacheRoot()+"/path")
			k, err := filedatacache.KeyFromPath(rpath)
			if err != nil {
				os.Remove(path)
				return nil
			}
			if md := fdc.Get(k); md != nil {
				szData[k.Size]++
				numData[len(md)]++
			} else {
				os.Remove(path)
			}

			return nil
		})
		if err != nil {
			fmt.Fprintln(os.Stderr, "Failed cache walk:", err)
			os.Exit(1)
		}
		fmt.Println("Data for file sizes:")
		for _, k := range sortedInt64Keys(szData) {
			fmt.Printf("%v: %v\n", k, szData[k])
		}
		fmt.Println("Data for number of Metadata entries:")
		for _, k := range sortedIntKeys(numData) {
			fmt.Printf("%v: %v\n", k, numData[k])
		}

	case "get":
		if md = fdc.Get(k); md != nil {
			fmt.Printf("Metadata entries: %d\n", len(md))
			for _, k := range md.SortedKeys() {
				v := md[k]
				fmt.Printf("%s: %s\n", k, v)
			}
		}

	case "add":
		md = fdc.Get(k)
		fallthrough
	case "put":
		if md == nil {
		}

		for _, kv := range os.Args[3:] {
			kvs := strings.SplitN(kv, ":", 2)
			if len(kvs) != 2 {
				fmt.Fprintln(os.Stderr, "Bad argument: ", kv)
				os.Exit(2)
			}
			k, v := kvs[0], kvs[1]
			md[k] = v
		}

		if err := fdc.Put(k, md); err != nil {
			fmt.Fprintln(os.Stderr, "Put:", err)
			os.Exit(2)
		}

	default:
		usage()
	}

}
