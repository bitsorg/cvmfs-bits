// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: Apache-2.0

package provenance

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/golang-jwt/jwt/v5"

	"cvmfs.io/prepub/pkg/observe"
)

// TestApplyClaims_VerifiedClaimsWin: every field the token provides overrides
// the caller's header, for GitHub and GitLab claim sets alike.
func TestApplyClaims_VerifiedClaimsWin(t *testing.T) {
	headers := func() *Record {
		return &Record{GitRepo: "evil/repo", GitSHA: "bad", GitRef: "refs/heads/evil",
			Actor: "mallory", PipelineID: "666", BuildSystem: "forged"}
	}

	gh := headers()
	applyClaims(gh, &OIDCClaims{
		RegisteredClaims: jwt.RegisteredClaims{Issuer: "https://token.actions.githubusercontent.com", Subject: "repo:o/r"},
		Repository:       "o/r", SHA: "abc", Ref: "refs/heads/main", Actor: "alice", RunID: "42", Workflow: "ci",
	})
	want := Record{GitRepo: "o/r", GitSHA: "abc", GitRef: "refs/heads/main", Actor: "alice", PipelineID: "42",
		BuildSystem: "github-actions", OIDCIssuer: "https://token.actions.githubusercontent.com",
		OIDCSubject: "repo:o/r", Verified: true}
	if !reflect.DeepEqual(*gh, want) {
		t.Errorf("github:\n got %+v\nwant %+v", *gh, want)
	}

	gl := headers()
	applyClaims(gl, &OIDCClaims{ProjectPath: "grp/proj", PipelineID: "7", UserLogin: "bob",
		SHA: "def", Ref: "main", CIConfigRef: "gitlab.com/grp/proj//.gitlab-ci.yml@refs/heads/main"})
	if gl.GitRepo != "grp/proj" || gl.PipelineID != "7" || gl.Actor != "bob" ||
		gl.BuildSystem != "gitlab-ci" || gl.GitSHA != "def" || gl.GitRef != "main" || !gl.Verified {
		t.Errorf("gitlab claims did not win over headers: %+v", *gl)
	}
}

// TestApplyClaims_ClearsUnattestedHeaders: a header for a field the token
// does not carry is dropped, not kept beside Verified=true.
func TestApplyClaims_ClearsUnattestedHeaders(t *testing.T) {
	rec := &Record{GitRepo: "evil/repo", GitSHA: "bad", GitRef: "refs/heads/evil",
		Actor: "mallory", PipelineID: "666", BuildSystem: "forged"}
	applyClaims(rec, &OIDCClaims{
		RegisteredClaims: jwt.RegisteredClaims{Issuer: "https://issuer.example", Subject: "s"},
		Repository:       "o/r",
	})
	want := Record{GitRepo: "o/r", OIDCIssuer: "https://issuer.example", OIDCSubject: "s", Verified: true}
	if !reflect.DeepEqual(*rec, want) {
		t.Errorf("\n got %+v\nwant %+v", *rec, want)
	}
}

// TestSubmit_KeepsSignedPayload: the payload kept on the record is exactly the
// bytes whose SHA-256 went to Rekor.
func TestSubmit_KeepsSignedPayload(t *testing.T) {
	var sentHash string
	rekor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req rekorEntryRequest
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &req)
		sentHash = req.Spec.Data.Hash.Value
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"uuid1":{"logIndex":1,"integratedTime":2,"verification":{"signedEntryTimestamp":"s"}}}`))
	}))
	defer rekor.Close()

	obs := &observe.Provider{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	p, err := New(Config{Enabled: true, RekorServer: rekor.URL}, t.TempDir(), obs)
	if err != nil {
		t.Fatal(err)
	}
	rec := &Record{JobID: "j", Repo: "r.cern.ch", CatalogHash: "c"}
	if err := p.Submit(context.Background(), rec); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(rec.SignedPayload)
	if len(rec.SignedPayload) == 0 || hex.EncodeToString(sum[:]) != sentHash {
		t.Errorf("SHA-256 of kept payload %x does not match the hash sent to Rekor %s", sum, sentHash)
	}
	if rec.RekorUUID != "uuid1" {
		t.Errorf("uuid = %q", rec.RekorUUID)
	}
}
