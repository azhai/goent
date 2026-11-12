package utils

import (
	"bytes"
	"cmp"
	"fmt"
	"slices"
)

type Imports struct {
	stdPackages   map[string]bool
	thirdPackages map[string]bool
}

func NewImports() *Imports {
	return &Imports{
		stdPackages:   make(map[string]bool),
		thirdPackages: make(map[string]bool),
	}
}

func (s *Imports) AddStdPackage(pkg string) {
	s.stdPackages[pkg] = true
}

func (s *Imports) GetStdPackage() []string {
	return sortedKeys(s.stdPackages)
}

// SortedKeys is slices.Sorted(maps.Keys(m)) without the Go 1.23 iterators.
func SortedKeys[K cmp.Ordered, V any](m map[K]V) []K {
	keys := make([]K, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

// sortedKeys is slices.Sorted(maps.Keys(m)) without the Go 1.23 iterators.
func sortedKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

func (s *Imports) AddThirdPackage(pkg string) {
	s.thirdPackages[pkg] = true
}

func (s *Imports) GetThirdPackage() []string {
	return sortedKeys(s.thirdPackages)
}

func (s *Imports) WriteTo(buf *bytes.Buffer) {
	stds, thirds := s.GetStdPackage(), s.GetThirdPackage()
	if len(stds) > 0 || len(thirds) > 0 {
		fmt.Fprintf(buf, "import (\n")
		if len(stds) > 0 {
			for _, imp := range stds {
				fmt.Fprintf(buf, "\t%s\n", imp)
			}
			if len(thirds) > 0 {
				fmt.Fprintf(buf, "\n")
			}
		}
		if len(thirds) > 0 {
			for _, imp := range thirds {
				fmt.Fprintf(buf, "\t%s\n", imp)
			}
		}
		fmt.Fprintf(buf, ")\n\n")
	}
}

func (s *Imports) String() string {
	buf := bytes.NewBufferString("")
	s.WriteTo(buf)
	return buf.String()
}
