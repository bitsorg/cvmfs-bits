package provenance

import (
	"crypto"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
	"path/filepath"
)

// loadOrGenerateKey loads the Ed25519 signing key from path, or generates and
// persists a new one if the file does not exist.
//
// Returns the private key (implements crypto.Signer) and the DER-encoded
// SubjectPublicKeyInfo representation of the corresponding public key.
// The DER encoding is suitable for inclusion in a Rekor hashedrekord entry.
func loadOrGenerateKey(path string) (crypto.Signer, []byte, error) {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return generateAndSaveKey(path)
	}
	if err != nil {
		return nil, nil, fmt.Errorf("reading signing key %q: %w", path, err)
	}

	block, _ := pem.Decode(data)
	if block == nil {
		return nil, nil, fmt.Errorf("no PEM block in signing key file %q", path)
	}

	rawKey, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, nil, fmt.Errorf("parsing PKCS#8 key from %q: %w", path, err)
	}

	priv, ok := rawKey.(ed25519.PrivateKey)
	if !ok {
		return nil, nil, fmt.Errorf("key in %q is not Ed25519 (got %T)", path, rawKey)
	}

	pubDER, err := marshalPublicKey(priv.Public())
	if err != nil {
		return nil, nil, err
	}
	return priv, pubDER, nil
}

func generateAndSaveKey(path string) (crypto.Signer, []byte, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, nil, fmt.Errorf("creating key directory: %w", err)
	}

	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("generating Ed25519 key: %w", err)
	}

	privDER, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return nil, nil, fmt.Errorf("marshalling private key: %w", err)
	}

	// Write with mode 0600 — private key must not be group- or world-readable.
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return nil, nil, fmt.Errorf("creating key file %q: %w", path, err)
	}
	if err := pem.Encode(f, &pem.Block{Type: "PRIVATE KEY", Bytes: privDER}); err != nil {
		f.Close()
		return nil, nil, fmt.Errorf("writing PEM key: %w", err)
	}
	if err := f.Close(); err != nil {
		return nil, nil, fmt.Errorf("closing key file: %w", err)
	}

	pubDER, err := marshalPublicKey(priv.Public())
	if err != nil {
		return nil, nil, err
	}
	return priv, pubDER, nil
}

func marshalPublicKey(pub crypto.PublicKey) ([]byte, error) {
	der, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		return nil, fmt.Errorf("marshalling public key to PKIX DER: %w", err)
	}
	return der, nil
}
