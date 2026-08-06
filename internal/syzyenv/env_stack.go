//go:build !syzy_extension

package syzyenv

import "os"

func getenv(key string) string { return os.Getenv(key) }

func environ() []string { return os.Environ() }
