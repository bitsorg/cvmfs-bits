package cvmfscatalog

import (
	"io/fs"
	"testing"
)

func TestMD5Path(t *testing.T) {
	tests := []struct {
		path string
		p1   int64
		p2   int64
	}{
		{
			path: "",
			p1:   338333539836370388,
			p2:   9098107892288553193,
		},
		{
			path: "/dir1",
			p1:   3166896040631616765,
			p2:   -9060999833566460932,
		},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			p1, p2 := MD5Path(tt.path)
			if p1 != tt.p1 || p2 != tt.p2 {
				t.Errorf("MD5Path(%q) = (%x, %x); want (%x, %x)",
					tt.path, p1, p2, tt.p1, tt.p2)
			}
		})
	}
}

func TestParentAbsPath(t *testing.T) {
	tests := []struct {
		path       string
		parentPath string
		found      bool
	}{
		{"", "", false},
		{"/foo", "", true},
		{"/foo/bar", "/foo", true},
		{"/a/b/c", "/a/b", true},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			parent, found := ParentAbsPath(tt.path)
			if parent != tt.parentPath || found != tt.found {
				t.Errorf("ParentAbsPath(%q) = (%q, %v); want (%q, %v)",
					tt.path, parent, found, tt.parentPath, tt.found)
			}
		})
	}
}

func TestUnixMode(t *testing.T) {
	tests := []struct {
		m    fs.FileMode
		want int64
	}{
		{fs.ModeDir | 0o755, 0o040755},
		{0o100644, 0o100644},
		{fs.ModeSymlink | 0o777, 0o120777},
	}

	for _, tt := range tests {
		got := UnixMode(tt.m)
		if got != tt.want {
			t.Errorf("UnixMode(%o) = %o; want %o", tt.m, got, tt.want)
		}
	}
}

func TestEntryFlags(t *testing.T) {
	tests := []struct {
		name string
		e    Entry
		want int
	}{
		{
			name: "regular file",
			e: Entry{
				Mode:     0o100644,
				HashAlgo: HashSha256,
				CompAlgo: CompZlib,
			},
			want: FlagFile | ((2-1)<<FlagPosHash) | (int(CompZlib)<<FlagPosComp),
		},
		{
			name: "directory",
			e: Entry{
				Mode: fs.ModeDir | 0o755,
			},
			want: FlagDir,
		},
		{
			name: "symlink",
			e: Entry{
				Mode: fs.ModeSymlink | 0o777,
			},
			want: FlagLink,
		},
		{
			name: "hidden file",
			e: Entry{
				Mode:     0o100644,
				IsHidden: true,
			},
			want: FlagFile | FlagHidden,
		},
		{
			name: "nested root",
			e: Entry{
				Mode:         fs.ModeDir | 0o755,
				IsNestedRoot: true,
			},
			want: FlagDir | FlagDirNestedRoot,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.e.Flags()
			if got != tt.want {
				t.Errorf("Entry.Flags() = %d; want %d (binary: %b vs %b)",
					got, tt.want, got, tt.want)
			}
		})
	}
}

func TestEntryHardlinks(t *testing.T) {
	tests := []struct {
		name  string
		group uint32
		count uint32
		want  int64
	}{
		{
			name:  "normal file",
			group: 0,
			count: 1,
			want:  1,
		},
		{
			name:  "hardlinked",
			group: 5,
			count: 3,
			want:  (int64(5) << 32) | 3,
		},
		{
			name:  "zero count defaults to 1",
			group: 2,
			count: 0,
			want:  (int64(2) << 32) | 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := Entry{
				HardlinkGroup: tt.group,
				LinkCount:     tt.count,
			}
			got := e.Hardlinks()
			if got != tt.want {
				t.Errorf("Entry.Hardlinks() = %d; want %d", got, tt.want)
			}
		})
	}
}

func TestHashAlgoFromFlags(t *testing.T) {
	tests := []struct {
		flags int
		want  HashAlgo
	}{
		{
			flags: (int(HashSha1) - 1) << FlagPosHash,
			want:  HashSha1,
		},
		{
			flags: (int(HashSha256) - 1) << FlagPosHash,
			want:  HashSha256,
		},
	}

	for _, tt := range tests {
		got := HashAlgoFromFlags(tt.flags)
		if got != tt.want {
			t.Errorf("HashAlgoFromFlags(%d) = %d; want %d", tt.flags, got, tt.want)
		}
	}
}
