package schemalog

import (
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
)

// Dial resolves a triple of CLI/env strings into a Log + closer.
// All-empty → returns (nil, nil, nil): DDL replication disabled.
//
//   - path:   local SQLite-file backend (multi-process safe via SQLite's writer lock)
//   - dial:   TCP addr of a remote schema log (follower mode)
//   - s3url:  bucket URL, e.g. https://bucket.s3.us-east-1.amazonaws.com?prefix=foo&region=us-east-1
//
// At most one of {path, dial, s3url} may be set.
func Dial(path, dial, s3url string) (Log, io.Closer, error) {
	chosen := 0
	for _, v := range []string{path, dial, s3url} {
		if v != "" {
			chosen++
		}
	}
	if chosen > 1 {
		return nil, nil, errors.New("schemalog: at most one of {path, dial, s3url} may be set")
	}
	switch {
	case dial != "":
		c, err := DialTCP(dial)
		if err != nil {
			return nil, nil, fmt.Errorf("schemalog: dial %q: %w", dial, err)
		}
		return c, c, nil
	case s3url != "":
		s, err := openS3URL(s3url)
		if err != nil {
			return nil, nil, fmt.Errorf("schemalog: open s3 %q: %w", s3url, err)
		}
		return s, s, nil
	case path != "":
		f, err := OpenFile(path)
		if err != nil {
			return nil, nil, fmt.Errorf("schemalog: open file %q: %w", path, err)
		}
		return f, f, nil
	}
	return nil, nil, nil
}

// openS3URL parses a bucket URL like
// "https://b.s3.us-east-1.amazonaws.com?prefix=syzy/foo&region=us-east-1"
// and returns a configured S3 backend. Credentials are read from the
// AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY environment variables.
func openS3URL(raw string) (*S3, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("parse url: %w", err)
	}
	q := u.Query()
	region := q.Get("region")
	prefix := q.Get("prefix")
	u.RawQuery = ""
	access := os.Getenv("AWS_ACCESS_KEY_ID")
	secret := os.Getenv("AWS_SECRET_ACCESS_KEY")
	if access == "" || secret == "" {
		return nil, errors.New("AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be set")
	}
	return OpenS3(S3Config{
		Endpoint:  u.String(),
		Region:    region,
		AccessKey: access,
		SecretKey: secret,
		Prefix:    prefix,
	})
}
