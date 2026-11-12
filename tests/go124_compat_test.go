package goent_test

import "sync"

// goRun starts fn on a new goroutine tracked by wg.
//
// It is the Go 1.22 stand-in for sync.WaitGroup.Go (added in Go 1.25): the
// call sites keep the `goRun(&wg, func() { ... })` shape, so the test bodies stay
// readable while the module still builds on the oldest toolchain we support.
func goRun(wg *sync.WaitGroup, fn func()) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		fn()
	}()
}
