package filedatacache

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
)

func TestFile(t *testing.T) {
	trootPath := "testdata/foo"

	if err := os.RemoveAll(trootPath); err != nil {
		t.Errorf("rmpath1(%s) error: %v\n", trootPath, err)
	}

	dname := fmt.Sprintf("%s/%04d", trootPath, 1)

	if err := os.MkdirAll(dname, 0755); err != nil {
		t.Errorf("mkpath(%s) error: %v\n", dname, err)
	}

	fdc := NewRoot("testdata/bar")

	kpath := dname + "/james"
	if err := ioutil.WriteFile(kpath, []byte("james"), 0644); err != nil {
		t.Errorf("WriteFile(%s) error: %v\n", dname, err)
	}

	k, _ := KeyFromPath(kpath)

	if md := fdc.Get(k); md != nil {
		t.Errorf("Found cache: %v\n", k.Path)
	}

	pmd := make(Metadata)
	pmd["len"] = "8"
	pmd["C"] = "JAM"

	if err := fdc.Put(k, pmd); err != nil {
		t.Errorf("Put(%s) error: %v\n", k.Path, err)
	}

	gmd := fdc.Get(k)
	if gmd == nil {
		t.Errorf("Not found cache: %v\n", k.Path)
	}
	if gmd["len"] != "8" {
		t.Errorf("Get(%s) error: %v != %v\n", k.Path, gmd["len"], "5")
	}
	if gmd["C"] != "JAM" {
		t.Errorf("Get(%s) error: %v != %v\n", k.Path, gmd["C"], "JAM")
	}
	if len(gmd) != 2 {
		t.Skipf("Size of Metadata != 2: %v: %v\n", k.Path, gmd)
	}
}
