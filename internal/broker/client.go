package broker

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// defaultConnectTimeout is how long New waits for the initial broker connection
// before returning an error.
const defaultConnectTimeout = 15 * time.Second

// defaultKeepAlive is the MQTT keep-alive interval sent to the broker.
const defaultKeepAlive = 30 * time.Second

// defaultReconnectWait is the minimum time between automatic reconnection
// attempts managed by the Paho library.
const defaultReconnectWait = 5 * time.Second

// Config holds the parameters needed to connect to the MQTT broker.
type Config struct {
	// BrokerURL is the broker address in Paho URL format, e.g.:
	//   "tls://broker.cern.ch:8883"   (mTLS — recommended for production)
	//   "tcp://localhost:1883"          (plain TCP — development only)
	// An empty BrokerURL means MQTT is disabled; callers should check this
	// before constructing a Client.
	BrokerURL string

	// ClientCert is the path to the PEM-encoded client TLS certificate.
	// Required when BrokerURL uses the "tls://" scheme.
	ClientCert string

	// ClientKey is the path to the PEM-encoded client TLS private key.
	// Required when BrokerURL uses the "tls://" scheme.
	ClientKey string

	// CACert is the path to the PEM-encoded CA certificate used to verify the
	// broker's server certificate.  When empty the system certificate pool is
	// used.
	CACert string

	// ClientID is the unique MQTT client identifier.  The broker uses this to
	// enforce per-node topic ACLs and to resume persistent sessions.
	// Defaults to the system hostname if empty.
	ClientID string
}

// Client wraps a Paho MQTT connection and provides typed publish/subscribe
// helpers used by the distributor and receiver.
//
// Client is safe to use from multiple goroutines.  The underlying Paho client
// handles automatic reconnection; callers do not need to handle CONNACK errors.
type Client struct {
	cfg            Config
	inner          mqtt.Client
	connected      atomic.Bool
	reconnectMu    sync.Mutex
	reconnectHook  func() // called on every reconnect (not the initial connect)
}

// SetReconnectHandler registers fn to be called each time the client
// successfully reconnects to the broker after a connection loss.  It is NOT
// called on the initial connect.  fn must not block.
//
// Typical use: receivers call this to re-publish their retained online-presence
// message, which the broker overwrites with the LWT on unexpected disconnect.
func (c *Client) SetReconnectHandler(fn func()) {
	c.reconnectMu.Lock()
	c.reconnectHook = fn
	c.reconnectMu.Unlock()
}

// New connects to the MQTT broker and returns a ready Client.
//
// The caller is responsible for calling Disconnect when the client is no longer
// needed.  If the broker is temporarily unavailable after the initial connect
// attempt the Paho library will reconnect in the background.
func New(cfg Config) (*Client, error) {
	if cfg.BrokerURL == "" {
		return nil, fmt.Errorf("broker: BrokerURL must not be empty")
	}
	clientID := cfg.ClientID
	if clientID == "" {
		if h, err := os.Hostname(); err == nil {
			clientID = h
		} else {
			clientID = "cvmfs-prepub"
		}
	}

	opts := mqtt.NewClientOptions().
		AddBroker(cfg.BrokerURL).
		SetClientID(clientID).
		SetKeepAlive(defaultKeepAlive).
		SetAutoReconnect(true).
		SetConnectRetryInterval(defaultReconnectWait).
		SetCleanSession(false) // persist subscriptions across reconnects

	// Configure mTLS when a client certificate is provided.
	tlsCfg, err := buildTLSConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("broker: building TLS config: %w", err)
	}
	if tlsCfg != nil {
		// Certificates were supplied but the URL scheme won't activate TLS —
		// the Paho library ignores SetTLSConfig for plain tcp:// or ws://.
		// Return an error rather than silently dropping the certs.
		if err := validateTLSScheme(cfg.BrokerURL); err != nil {
			return nil, err
		}
		opts.SetTLSConfig(tlsCfg)
	}

	c := &Client{cfg: cfg}

	opts.SetOnConnectHandler(func(_ mqtt.Client) {
		c.connected.Store(true)
	})
	opts.SetConnectionLostHandler(func(_ mqtt.Client, err error) {
		c.connected.Store(false)
		// Reconnection is handled automatically by Paho; nothing to do here.
		// Callers that care about transient disconnects can poll IsConnected.
		_ = err
	})

	inner := mqtt.NewClient(opts)
	tok := inner.Connect()
	if !tok.WaitTimeout(defaultConnectTimeout) {
		return nil, fmt.Errorf("broker: connect to %q timed out after %s", cfg.BrokerURL, defaultConnectTimeout)
	}
	if err := tok.Error(); err != nil {
		return nil, fmt.Errorf("broker: connecting to %q: %w", cfg.BrokerURL, err)
	}

	c.inner = inner
	c.connected.Store(true)
	return c, nil
}

// Disconnect gracefully closes the MQTT connection.
// quiesceMs is the number of milliseconds to wait for in-flight messages to
// drain before forcibly closing the connection.
func (c *Client) Disconnect(quiesceMs uint) {
	c.inner.Disconnect(quiesceMs)
	c.connected.Store(false)
}

// IsConnected reports whether the client is currently connected to the broker.
func (c *Client) IsConnected() bool {
	return c.connected.Load()
}

// Publish serialises v as JSON and publishes it to topic with the given QoS and
// retained flag.  It blocks until the broker acknowledges the publish (for QoS
// ≥ 1) or until the operation times out.
func (c *Client) Publish(topic string, qos byte, retained bool, v any) error {
	payload, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("broker: marshalling payload for topic %q: %w", topic, err)
	}
	tok := c.inner.Publish(topic, qos, retained, payload)
	if !tok.WaitTimeout(10 * time.Second) {
		return fmt.Errorf("broker: publish to %q timed out", topic)
	}
	return tok.Error()
}

// PublishLWT configures a Last-Will-and-Testament message on the client
// options.  This must be called before New — it is provided here as a helper
// to build the options struct from a typed message.
//
// This function is intentionally not a method on Client (which is already
// connected); use ConfigureLWT on mqtt.ClientOptions directly before calling
// New when LWT semantics are needed.
func ConfigureLWT(opts *mqtt.ClientOptions, topic string, qos byte, retained bool, v any) error {
	payload, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("broker: marshalling LWT payload for topic %q: %w", topic, err)
	}
	opts.SetWill(topic, string(payload), qos, retained)
	return nil
}

// Subscribe registers handler to receive messages on the given topic filter.
// handler is called in a goroutine managed by the Paho library; implementations
// must be goroutine-safe.
func (c *Client) Subscribe(topicFilter string, qos byte, handler func(msg *Message)) error {
	wrapper := func(_ mqtt.Client, m mqtt.Message) {
		handler(&Message{
			Topic:   m.Topic(),
			Payload: m.Payload(),
		})
	}
	tok := c.inner.Subscribe(topicFilter, qos, wrapper)
	if !tok.WaitTimeout(10 * time.Second) {
		return fmt.Errorf("broker: subscribe to %q timed out", topicFilter)
	}
	return tok.Error()
}

// Unsubscribe removes the subscription for topicFilter.
func (c *Client) Unsubscribe(topicFilter string) error {
	tok := c.inner.Unsubscribe(topicFilter)
	if !tok.WaitTimeout(10 * time.Second) {
		return fmt.Errorf("broker: unsubscribe from %q timed out", topicFilter)
	}
	return tok.Error()
}

// Message is delivered to Subscribe handlers.
type Message struct {
	Topic   string
	Payload []byte
}

// Decode unmarshals the JSON payload into v.
func (m *Message) Decode(v any) error {
	if err := json.Unmarshal(m.Payload, v); err != nil {
		return fmt.Errorf("broker: decoding message on topic %q: %w", m.Topic, err)
	}
	return nil
}

// validateTLSScheme returns an error when the BrokerURL scheme does not
// activate TLS in the Paho library (i.e. is not "tls", "ssl", or "wss").
// This prevents TLS certificates from being silently ignored on plain
// tcp:// or ws:// connections where Paho would skip the TLS handshake.
func validateTLSScheme(brokerURL string) error {
	scheme, _, ok := strings.Cut(brokerURL, "://")
	if !ok {
		return fmt.Errorf("broker: malformed BrokerURL (missing \"://\"): %q", brokerURL)
	}
	switch scheme {
	case "tls", "ssl", "mqtts", "wss":
		// All four schemes cause Paho to perform a TLS handshake.
		return nil
	default:
		return fmt.Errorf("broker: TLS certificates configured but BrokerURL scheme is %q — use tls://, mqtts://, or wss://", scheme)
	}
}

// buildTLSConfig constructs a *tls.Config from the broker Config.
// Returns nil (no TLS) when neither cert nor CA is specified, which is
// appropriate for plain tcp:// connections.
func buildTLSConfig(cfg Config) (*tls.Config, error) {
	hasCert := cfg.ClientCert != "" || cfg.ClientKey != ""
	hasCA := cfg.CACert != ""
	if !hasCert && !hasCA {
		return nil, nil // plain TCP, no TLS
	}

	tlsCfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}

	// Load the CA certificate for server verification.
	if hasCA {
		pem, err := os.ReadFile(cfg.CACert)
		if err != nil {
			return nil, fmt.Errorf("reading CA cert %q: %w", cfg.CACert, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("parsing CA cert %q: no valid PEM blocks found", cfg.CACert)
		}
		tlsCfg.RootCAs = pool
	}

	// Load the client certificate for mTLS.
	if hasCert {
		if cfg.ClientCert == "" || cfg.ClientKey == "" {
			return nil, fmt.Errorf("both --broker-client-cert and --broker-client-key must be set together")
		}
		cert, err := tls.LoadX509KeyPair(cfg.ClientCert, cfg.ClientKey)
		if err != nil {
			return nil, fmt.Errorf("loading client keypair (%q, %q): %w", cfg.ClientCert, cfg.ClientKey, err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}

	return tlsCfg, nil
}

// NewWithLWT is like New but also configures a Last-Will-and-Testament on the
// connection.  The LWT message (lwtPayload serialised as JSON) is published to
// lwtTopic by the broker if the connection drops unexpectedly.
//
// This is the primary constructor used by receivers: they publish a retained
// PresenceMessage{Online:true} on connect and configure
// PresenceMessage{Online:false} as the LWT, so that the broker automatically
// marks them offline without any application-level action.
func NewWithLWT(cfg Config, lwtTopic string, lwtQoS byte, lwtRetained bool, lwtPayload any) (*Client, error) {
	if cfg.BrokerURL == "" {
		return nil, fmt.Errorf("broker: BrokerURL must not be empty")
	}
	clientID := cfg.ClientID
	if clientID == "" {
		if h, err := os.Hostname(); err == nil {
			clientID = h
		} else {
			clientID = "cvmfs-prepub"
		}
	}

	opts := mqtt.NewClientOptions().
		AddBroker(cfg.BrokerURL).
		SetClientID(clientID).
		SetKeepAlive(defaultKeepAlive).
		SetAutoReconnect(true).
		SetConnectRetryInterval(defaultReconnectWait).
		SetCleanSession(false)

	tlsCfg, err := buildTLSConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("broker: building TLS config: %w", err)
	}
	if tlsCfg != nil {
		if err := validateTLSScheme(cfg.BrokerURL); err != nil {
			return nil, err
		}
		opts.SetTLSConfig(tlsCfg)
	}

	// Configure LWT before connecting.
	if err := ConfigureLWT(opts, lwtTopic, lwtQoS, lwtRetained, lwtPayload); err != nil {
		return nil, err
	}

	c := &Client{cfg: cfg}

	// isInitialConnect distinguishes the first connect from subsequent reconnects.
	// Swap-on-first-call: returns true on the first invocation, false thereafter.
	var initialConnectDone atomic.Bool
	opts.SetOnConnectHandler(func(_ mqtt.Client) {
		c.connected.Store(true)
		if initialConnectDone.Swap(true) {
			// This is a reconnect — invoke the registered hook so the caller can
			// republish any retained messages that were overwritten by the LWT.
			c.reconnectMu.Lock()
			hook := c.reconnectHook
			c.reconnectMu.Unlock()
			if hook != nil {
				hook()
			}
		}
	})
	opts.SetConnectionLostHandler(func(_ mqtt.Client, _ error) {
		c.connected.Store(false)
	})

	inner := mqtt.NewClient(opts)
	tok := inner.Connect()
	if !tok.WaitTimeout(defaultConnectTimeout) {
		return nil, fmt.Errorf("broker: connect to %q timed out after %s", cfg.BrokerURL, defaultConnectTimeout)
	}
	if err := tok.Error(); err != nil {
		return nil, fmt.Errorf("broker: connecting to %q: %w", cfg.BrokerURL, err)
	}

	c.inner = inner
	c.connected.Store(true)
	return c, nil
}
