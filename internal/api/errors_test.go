package api

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"testing"
)

func TestClassOf_Nil(t *testing.T) {
	if got := ClassOf(nil); got != ErrClassInternal {
		t.Errorf("ClassOf(nil) = %v, want Internal", got)
	}
}

func TestClassOf_ExplicitClassification(t *testing.T) {
	cases := []struct {
		class ErrClass
		want  ErrClass
	}{
		{ErrClassTransient, ErrClassTransient},
		{ErrClassPermanent, ErrClassPermanent},
		{ErrClassInternal, ErrClassInternal},
	}
	for _, tc := range cases {
		err := Classify(tc.class, errors.New("some error"))
		if got := ClassOf(err); got != tc.want {
			t.Errorf("ClassOf(Classify(%v, ...)) = %v, want %v", tc.class, got, tc.want)
		}
	}
}

func TestClassOf_WrappedClassification(t *testing.T) {
	inner := Classify(ErrClassTransient, errors.New("network blip"))
	outer := fmt.Errorf("pipeline: %w", inner)

	if got := ClassOf(outer); got != ErrClassTransient {
		t.Errorf("ClassOf(wrapped transient) = %v, want Transient", got)
	}
}

func TestClassOf_NetworkErrorIsTransient(t *testing.T) {
	// *url.Error — returned by http.Client on connection failure.
	urlErr := &url.Error{Op: "Get", URL: "https://example.com", Err: errors.New("refused")}
	if got := ClassOf(urlErr); got != ErrClassTransient {
		t.Errorf("ClassOf(*url.Error) = %v, want Transient", got)
	}

	// *net.OpError — lower-level network error.
	opErr := &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}
	if got := ClassOf(opErr); got != ErrClassTransient {
		t.Errorf("ClassOf(*net.OpError) = %v, want Transient", got)
	}
}

func TestClassOf_UnknownErrorIsInternal(t *testing.T) {
	err := errors.New("something unexpected")
	if got := ClassOf(err); got != ErrClassInternal {
		t.Errorf("ClassOf(plain error) = %v, want Internal", got)
	}
}

func TestClassify_NilPassthrough(t *testing.T) {
	if err := Classify(ErrClassTransient, nil); err != nil {
		t.Errorf("Classify(Transient, nil) = %v, want nil", err)
	}
}

func TestClassifiedError_Unwrap(t *testing.T) {
	cause := errors.New("root cause")
	classified := Classify(ErrClassPermanent, cause)

	if !errors.Is(classified, cause) {
		t.Error("errors.Is should find the root cause through ClassifiedError")
	}
}

func TestErrClassString(t *testing.T) {
	cases := map[ErrClass]string{
		ErrClassTransient: "transient",
		ErrClassPermanent: "permanent",
		ErrClassInternal:  "internal",
	}
	for class, want := range cases {
		if got := class.String(); got != want {
			t.Errorf("ErrClass(%d).String() = %q, want %q", class, got, want)
		}
	}
}
