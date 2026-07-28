package main

import (
	"fmt"
	"os"

	"github.com/wjordan/syzy/internal/buildinfo"
)

func versionCmd(args []string) error {
	if len(args) > 0 {
		return fmt.Errorf("version takes no arguments")
	}
	fmt.Fprint(os.Stdout, buildinfo.Full())
	return nil
}
