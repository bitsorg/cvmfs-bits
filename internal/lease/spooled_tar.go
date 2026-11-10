// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package lease

import (
	"archive/tar"
	"bufio"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"
)

// EncodingGzip names a spooled payload that is a gzip-compressed tar.
const EncodingGzip = "gzip"

// errTarTooLarge stops a spooled tar that grows past its limit: an error,
// never a short read, which could pass for a complete archive.
var errTarTooLarge = errors.New("the spooled tar is larger than allowed")

// spooledTar reads a spooled tar, decompressing it when it is encoded; n counts the
// tar's bytes read so far.
type spooledTar struct {
	f     *os.File
	r     io.Reader
	n     int64
	limit int64 // of the tar's bytes when encoded; 0 for none
}

// openSpooledTar opens the spooled payload at p as a tar: as it is when encoding is empty,
// else decompressed, refusing more than limit bytes of tar (0: no limit).
func openSpooledTar(p, encoding string, limit int64) (*spooledTar, error) {
	if encoding != "" && encoding != EncodingGzip {
		return nil, fmt.Errorf("payload encoding %q is not supported", encoding)
	}
	f, err := os.Open(p)
	if err != nil {
		return nil, err
	}
	pl := &spooledTar{f: f, r: f}
	if encoding == EncodingGzip {
		zr, err := gzip.NewReader(bufio.NewReaderSize(f, 1<<20))
		if err != nil {
			f.Close()
			return nil, fmt.Errorf("%s: %w", p, err)
		}
		pl.r, pl.limit = zr, limit
	}
	return pl, nil
}

func (p *spooledTar) Read(b []byte) (int, error) {
	n, err := p.r.Read(b)
	p.n += int64(n)
	if p.limit > 0 && p.n > p.limit {
		return n, errTarTooLarge
	}
	return n, err
}

func (p *spooledTar) Close() error { return p.f.Close() }

// writeSpooledTar writes the spooled payload at p to w as the tar it holds,
// entry by entry: the end of the archive is written only once the whole stream
// has been read and checked (gzip's CRC), so a bad one never looks complete.
// It returns the tar's bytes read.
func writeSpooledTar(w io.Writer, p, encoding string, limit int64) (int64, error) {
	pl, err := openSpooledTar(p, encoding, limit)
	if err != nil {
		return 0, err
	}
	defer pl.Close()
	tr, tw := tar.NewReader(pl), tar.NewWriter(w)
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return pl.n, fmt.Errorf("reading %s: %w", p, err)
		}
		if hdr.Typeflag == tar.TypeGNUSparse {
			return pl.n, fmt.Errorf("tar entry %q: sparse files are not taken compressed", hdr.Name)
		}
		hdr.Format = tar.FormatUnknown // the writer picks what the names need
		if err := tw.WriteHeader(hdr); err != nil {
			return pl.n, err
		}
		if _, err := io.Copy(tw, tr); err != nil {
			return pl.n, fmt.Errorf("reading %s: %w", p, err)
		}
	}
	if _, err := io.Copy(io.Discard, pl); err != nil {
		return pl.n, fmt.Errorf("reading %s: %w", p, err)
	}
	return pl.n, tw.Close()
}
