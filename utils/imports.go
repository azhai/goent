package utils

import (
	"bytes"
	"fmt"
	"maps"
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
	return slices.Sorted(maps.Keys(s.stdPackages))
}

func (s *Imports) AddThirdPackage(pkg string) {
	s.thirdPackages[pkg] = true
}

func (s *Imports) GetThirdPackage() []string {
	return slices.Sorted(maps.Keys(s.thirdPackages))
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
