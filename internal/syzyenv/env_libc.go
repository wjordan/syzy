//go:build syzy_extension

package syzyenv

/*
#include <stdlib.h>

// The live environment pointer libc maintains. A host process that
// relocates its environment repoints this symbol; the stack block Go
// read at library load time does not move with it.
extern char **environ;
*/
import "C"

import "unsafe"

func getenv(key string) string {
	ckey := C.CString(key)
	defer C.free(unsafe.Pointer(ckey))
	if v := C.getenv(ckey); v != nil {
		return C.GoString(v)
	}
	return ""
}

func environ() []string {
	var out []string
	for p := C.environ; p != nil && *p != nil; p = (**C.char)(unsafe.Add(unsafe.Pointer(p), unsafe.Sizeof(*p))) {
		out = append(out, C.GoString(*p))
	}
	return out
}
