package digest

import (
	"testing"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
)

func TestFromBytes(t *testing.T) {
	d := FromBytes([]byte("hello"))
	const want = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
	if d.Hash != want {
		t.Errorf("hash = %s, want %s", d.Hash, want)
	}
	if d.SizeBytes != 5 {
		t.Errorf("size = %d, want 5", d.SizeBytes)
	}
}

func TestEmpty(t *testing.T) {
	e := Empty()
	if e.Hash != EmptyHash || e.SizeBytes != 0 {
		t.Errorf("Empty() = %v", e)
	}
	if !IsEmpty(FromBytes(nil)) {
		t.Error("FromBytes(nil) should be empty")
	}
	if IsEmpty(FromBytes([]byte("x"))) {
		t.Error("non-empty blob reported as empty")
	}
}

func TestValidate(t *testing.T) {
	if err := Validate(FromBytes([]byte("ok"))); err != nil {
		t.Errorf("valid digest rejected: %v", err)
	}
	bad := []*repb.Digest{
		nil,
		{Hash: "short", SizeBytes: 1},
		{Hash: "zzzz24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824", SizeBytes: 1},
		{Hash: EmptyHash, SizeBytes: -1},
	}
	for i, d := range bad {
		if err := Validate(d); err == nil {
			t.Errorf("case %d: expected validation error", i)
		}
	}
}
