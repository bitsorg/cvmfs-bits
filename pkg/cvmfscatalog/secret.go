package cvmfscatalog

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io/fs"
)

const (
	SharesRoot = ".shares" // hidden top-level dir
	TokenLen   = 32        // bytes → 64 hex chars
)

// NewToken generates a cryptographically random 64-hex-char share token.
func NewToken() (string, error) {
	bytes := make([]byte, TokenLen)
	if _, err := rand.Read(bytes); err != nil {
		return "", fmt.Errorf("generating token: %w", err)
	}
	return hex.EncodeToString(bytes), nil
}

// SharePath returns the CVMFS path for a share: ".shares/<token>/<contentPath>"
// contentPath should not have a leading slash.
func SharePath(token, contentPath string) string {
	return SharesRoot + "/" + token + "/" + contentPath
}

// SharesDirEntry returns the Entry for the .shares root directory (always hidden).
func SharesDirEntry(mtime int64) Entry {
	return Entry{
		FullPath:   SharesRoot,
		Name:       SharesRoot,
		Mode:       fs.ModeDir | 0o700,
		Size:       4096,
		Mtime:      mtime,
		MtimeNs:    0,
		UID:        0,
		GID:        0,
		LinkCount:  1,
		IsHidden:   true,
		HashAlgo:   HashSha256,
		CompAlgo:   CompZlib,
	}
}

// TokenDirEntry returns the Entry for .shares/<token>/ directory (always hidden).
func TokenDirEntry(token string, mtime int64) Entry {
	return Entry{
		FullPath:   SharesRoot + "/" + token,
		Name:       token,
		Mode:       fs.ModeDir | 0o700,
		Size:       4096,
		Mtime:      mtime,
		MtimeNs:    0,
		UID:        0,
		GID:        0,
		LinkCount:  1,
		IsHidden:   true,
		HashAlgo:   HashSha256,
		CompAlgo:   CompZlib,
	}
}
