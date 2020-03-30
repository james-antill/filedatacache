package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"

	"github.com/james-antill/filedatacache"
)

func usage() {
	fmt.Fprintln(os.Stderr, "Usage: fdc <summary|get|put|add> file [key:value]...")
	os.Exit(2)
}

func intKeys(d map[int]int64) []int {
	ret := make([]int, 0, len(d))

	for k := range d {
		ret = append(ret, k)
	}

	return ret
}

func sortedIntKeys(d map[int]int64) []int {
	ret := intKeys(d)
	sort.Ints(ret)

	return ret
}

func int64Keys(d map[int64]int64) []int64 {
	ret := make([]int64, 0, len(d))

	for k := range d {
		ret = append(ret, k)
	}

	return ret
}

func sortedInt64Keys(d map[int64]int64) []int64 {
	ret := int64Keys(d)

	sort.Slice(ret, func(i, j int) bool { return ret[i] < ret[j] })

	return ret
}

func minMax(entries []int64) (int64, int64) {
	if len(entries) < 1 {
		return 0, 0
	}

	min := entries[0]
	max := entries[0]
	for _, v := range entries {
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
	}

	return min, max
}

func histoBuckets(entries []int64, size int) (map[int64]int64,
	int64, int64, int64) {
	ret := make(map[int64]int64)

	if size < 1 {
		size = 1
	}

	min, max := minMax(entries)
	diff := max - min
	off := diff / int64(size)
	for _, v := range entries {
		nv := (v - min) / off
		if nv != int64(size) { // The last value overflows...
			nv++
		}
		ret[v] = nv
	}

	return ret, min, off, max
}

func histoCombine(entries map[int64]int64,
	hist map[int64]int64) map[int64]int64 {
	ret := make(map[int64]int64)

	for k, v := range entries {
		ret[hist[k]] += v
	}

	return ret
}

func num2hashes(num int64, min, max, hashes int64) string {
	if num < min {
		num = min
	}
	if num > max {
		num = max
	}

	diff := max - min
	off := diff / hashes
	pc := (num - min) / off

	return strings.Repeat("#", int(pc))
}

const workersNum = 32

// KMD is a holder for the key and metadata, because no tuples for channels
type KMD struct {
	md filedatacache.Metadata
	k  filedatacache.Key
}

func main() {

	fdc := filedatacache.New()
	if fdc == nil {
		fmt.Fprintln(os.Stderr, "Can't find Cache.")
		os.Exit(1)
	}

	var k filedatacache.Key

	if len(os.Args) < 2 {
		usage()
	}

	switch os.Args[1] {
	case "sum":
		fallthrough
	case "summary":
		break

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
		var numFiles int64
		var numDeletes int64

		szData := make(map[int64]int64)
		numData := make(map[int]int64)

		sem := make(chan int, workersNum)

		found := make(chan *KMD, 1)
		go func() {
			for kmd := range found {
				k := kmd.k
				md := kmd.md

				szData[k.Size]++
				numData[len(md)]++
				numFiles++
			}
		}()

		err := filepath.Walk(fdc.CacheRoot(), func(path string, fi os.FileInfo, err error) error {
			if fi.IsDir() {
				return nil
			}

			sem <- 1
			go func() {
				defer func() { <-sem }()

				rpath := strings.TrimPrefix(path, fdc.CacheRoot()+"/path")
				k, err := filedatacache.KeyFromPath(rpath)
				if err != nil {
					atomic.AddInt64(&numDeletes, 1)
					os.Remove(path)
				}
				if md := fdc.Get(k); md != nil {
					found <- &KMD{k: k, md: md}
				} else {
					atomic.AddInt64(&numDeletes, 1)
					os.Remove(path)
				}
			}()

			return nil
		})
		for i := 0; i < workersNum; i++ {
			sem <- 2
		}
		close(sem)
		close(found)

		if err != nil {
			fmt.Fprintln(os.Stderr, "Failed cache walk:", err)
			os.Exit(1)
		}
		fmt.Println("Data for file sizes:")
		buck, firstk, diff, lastk := histoBuckets(int64Keys(szData), 8)
		prevk := firstk
		vals := histoCombine(szData, buck)
		kmaxlen := len(fmt.Sprintf("%v", lastk))
		//		for _, k := range sortedInt64Keys(vals) {
		var vmaxnum int64
		for k := int64(1); k <= 8; k++ {
			if vmaxnum < vals[k] {
				vmaxnum = vals[k]
			}
		}
		vmaxlen := len(fmt.Sprintf("%v", vmaxnum))
		for k := int64(1); k <= 8; k++ {
			nk := prevk + diff
			if k == 8 {
				nk = lastk
			}
			fmt.Printf("%*v-%*v: %*v %s\n", kmaxlen, prevk, kmaxlen, nk,
				vmaxlen, vals[k], num2hashes(vals[k], 0, vmaxnum, 40))
			prevk = nk
		}
		fmt.Println("Data for number of Metadata entries:")
		for _, k := range sortedIntKeys(numData) {
			fmt.Printf("%v: %v\n", k, numData[k])
		}
		fmt.Println("Number of files in cache:", numFiles)
		fmt.Println("Number of files deleted:", numDeletes)

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
