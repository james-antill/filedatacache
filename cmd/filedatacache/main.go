package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/james-antill/filedatacache"
)

func usage() {
	fmt.Fprintln(os.Stderr, "Usage: fdc <get|put|add> file [key:value]...")
	os.Exit(2)

}

func main() {

	fdc := filedatacache.New()

	if len(os.Args) < 3 {
		usage()
	}

	k, _ := filedatacache.KeyFromPath(os.Args[2])

	var md filedatacache.Metadata

	switch os.Args[1] {
	case "get":
		if md = fdc.Get(k); md != nil {
			fmt.Printf("Metadata entries: %d\n", len(md))
			for k, v := range md {
				fmt.Printf("%s: %s\n", k, v)
			}

		}

	case "add":
		md = fdc.Get(k)
		fallthrough
	case "put":
		if md == nil {
			md = make(filedatacache.Metadata)
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
