package awsenv

import (
	"os"
	"testing"
)

func TestSanitizeTrimsWhitespace(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", " AKIAEXAMPLE")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "secret/value ")
	t.Setenv("AWS_REGION", "us-east-1") // already clean

	fixed := Sanitize()

	if got := os.Getenv("AWS_ACCESS_KEY_ID"); got != "AKIAEXAMPLE" {
		t.Errorf("AWS_ACCESS_KEY_ID = %q, want %q", got, "AKIAEXAMPLE")
	}
	if got := os.Getenv("AWS_SECRET_ACCESS_KEY"); got != "secret/value" {
		t.Errorf("AWS_SECRET_ACCESS_KEY = %q, want %q", got, "secret/value")
	}

	wantFixed := map[string]bool{"AWS_ACCESS_KEY_ID": true, "AWS_SECRET_ACCESS_KEY": true}
	if len(fixed) != len(wantFixed) {
		t.Fatalf("fixed = %v, want the two whitespace vars", fixed)
	}
	for _, name := range fixed {
		if !wantFixed[name] {
			t.Errorf("unexpected var reported fixed: %q", name)
		}
	}
}

func TestSanitizeLeavesCleanValuesUntouched(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "AKIACLEAN")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "cleansecret")
	os.Unsetenv("AWS_SESSION_TOKEN")
	os.Unsetenv("AWS_REGION")
	os.Unsetenv("AWS_DEFAULT_REGION")

	if fixed := Sanitize(); len(fixed) != 0 {
		t.Errorf("Sanitize reported fixes on clean env: %v", fixed)
	}
}
