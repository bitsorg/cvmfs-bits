package api

import (
	"errors"
	"fmt"
	"net"
	"net/url"
)

// ErrClass categorises a job error so operators can set targeted alerts:
//
//   - Transient  — worth retrying automatically (network blip, timeout, 503).
//   - Permanent  — retrying will not help (bad input, 4xx from gateway).
//   - Internal   — bug or misconfiguration in this service; needs human attention.
type ErrClass int

const (
	ErrClassInternal  ErrClass = iota // default zero value
	ErrClassTransient ErrClass = iota
	ErrClassPermanent ErrClass = iota
)

func (c ErrClass) String() string {
	switch c {
	case ErrClassTransient:
		return "transient"
	case ErrClassPermanent:
		return "permanent"
	default:
		return "internal"
	}
}

// ClassifiedError wraps an error together with its class.
type ClassifiedError struct {
	Cause error
	Class ErrClass
}

func (e *ClassifiedError) Error() string {
	return fmt.Sprintf("[%s] %v", e.Class, e.Cause)
}

func (e *ClassifiedError) Unwrap() error {
	return e.Cause
}

// Classify wraps err with the given class.  It is a convenience constructor
// for callers that know the class at the point the error is created.
func Classify(class ErrClass, err error) error {
	if err == nil {
		return nil
	}
	return &ClassifiedError{Cause: err, Class: class}
}

// ClassOf inspects err (including its chain) and returns the most specific
// ErrClass.  If no ClassifiedError is found in the chain, heuristics based on
// the error type are applied:
//
//   - *url.Error, *net.OpError → Transient (network-level failure)
//   - net.Error with Timeout() → Transient
//   - everything else          → Internal
func ClassOf(err error) ErrClass {
	if err == nil {
		return ErrClassInternal
	}

	// Unwrap the chain looking for an explicit classification.
	var ce *ClassifiedError
	if errors.As(err, &ce) {
		return ce.Class
	}

	// Heuristic: network-level errors are almost always transient.
	var netErr net.Error
	if errors.As(err, &netErr) {
		return ErrClassTransient
	}
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		return ErrClassTransient
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return ErrClassTransient
	}

	return ErrClassInternal
}
