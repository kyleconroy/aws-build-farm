// Package awsenv defends against malformed AWS credential environment
// variables.
//
// Some environments inject credentials with stray surrounding whitespace (for
// example a leading space on AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY). The AWS
// SigV4 signer copies those bytes verbatim into the Authorization header, which
// AWS then rejects with "IncompleteSignature: Invalid key=value pair (missing
// equal-sign) in Authorization header" even though the underlying key material
// is valid.
//
// AWS access key IDs, secret keys, session tokens, and region names never
// contain leading or trailing whitespace, so trimming it is always safe and
// fixes the broken environment in place before the SDK's env credential
// provider reads it.
package awsenv

import (
	"os"
	"strings"
)

// sanitizedVars are the AWS environment variables whose values must never carry
// surrounding whitespace.
var sanitizedVars = []string{
	"AWS_ACCESS_KEY_ID",
	"AWS_SECRET_ACCESS_KEY",
	"AWS_SESSION_TOKEN",
	"AWS_REGION",
	"AWS_DEFAULT_REGION",
}

// Sanitize trims surrounding whitespace from the well-known AWS credential and
// region environment variables, rewriting any that changed so the AWS SDK reads
// clean values. It returns the names of the variables it had to fix.
//
// Call it once at process startup, before loading AWS config.
func Sanitize() []string {
	var fixed []string
	for _, name := range sanitizedVars {
		raw, ok := os.LookupEnv(name)
		if !ok {
			continue
		}
		trimmed := strings.TrimSpace(raw)
		if trimmed != raw {
			os.Setenv(name, trimmed)
			fixed = append(fixed, name)
		}
	}
	return fixed
}
