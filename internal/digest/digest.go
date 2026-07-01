// Package digest provides helpers for REAPI v2 content digests.
//
// REAPI identifies every blob in the CAS by a Digest: the SHA-256 hash of the
// blob's contents (lower-case hex) together with its size in bytes. This
// implementation only supports SHA-256, which is the REAPI default digest
// function.
package digest

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
)

// EmptyHash is the SHA-256 of zero bytes. The empty blob is always considered
// present in the CAS.
const EmptyHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

// Empty is the digest of the zero-length blob.
func Empty() *repb.Digest {
	return &repb.Digest{Hash: EmptyHash, SizeBytes: 0}
}

// FromBytes computes the digest of b.
func FromBytes(b []byte) *repb.Digest {
	sum := sha256.Sum256(b)
	return &repb.Digest{Hash: hex.EncodeToString(sum[:]), SizeBytes: int64(len(b))}
}

// IsEmpty reports whether d refers to the zero-length blob.
func IsEmpty(d *repb.Digest) bool {
	return d.GetSizeBytes() == 0 && d.GetHash() == EmptyHash
}

// Key returns a stable "hash/size" string identifying the digest.
func Key(d *repb.Digest) string {
	return fmt.Sprintf("%s/%d", d.GetHash(), d.GetSizeBytes())
}

// Equal reports whether a and b identify the same blob.
func Equal(a, b *repb.Digest) bool {
	return a.GetHash() == b.GetHash() && a.GetSizeBytes() == b.GetSizeBytes()
}

// String returns the canonical "hash/size" rendering of d, matching Key.
func String(d *repb.Digest) string {
	return Key(d)
}

// Validate checks that d is a structurally valid SHA-256 digest.
func Validate(d *repb.Digest) error {
	if d == nil {
		return fmt.Errorf("digest is nil")
	}
	if len(d.Hash) != sha256.Size*2 {
		return fmt.Errorf("invalid sha256 hash length %d for %q", len(d.Hash), d.Hash)
	}
	if _, err := hex.DecodeString(d.Hash); err != nil {
		return fmt.Errorf("hash is not valid hex: %w", err)
	}
	if d.SizeBytes < 0 {
		return fmt.Errorf("negative size_bytes %d", d.SizeBytes)
	}
	return nil
}
