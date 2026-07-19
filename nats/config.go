package nats

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
)

// Default connection-tuning values, applied when the corresponding Config
// fields are zero.
const (
	defaultConnectionName = "core-nats"
	defaultDialTimeout    = 10 * time.Second
	defaultReconnectWait  = 2 * time.Second
)

// MaxReconnectsForever is the sentinel for "retry reconnection indefinitely".
// Wrap with IntPtr for use in Config.MaxReconnects: nats.IntPtr(nats.MaxReconnectsForever).
const MaxReconnectsForever = -1

// IntPtr returns a pointer to v. Helper for filling *int fields like
// Config.MaxReconnects with a literal value.
func IntPtr(v int) *int { return new(v) }

// Config describes NATS connection parameters. All authentication fields are
// optional; supply at most one of (User+Password), Token, or CredsFile.
type Config struct {
	// URL is the NATS server URL, e.g. "nats://localhost:4222".
	// Required unless WithConnection is used.
	URL string

	// User is the username for basic authentication.
	User string

	// Password is the password for basic authentication.
	Password string

	// Token is the token for token-based authentication.
	Token string

	// CredsFile is the path to a NATS credentials file (.creds).
	CredsFile string

	// ConnectionName is reported to the NATS server and is visible in the
	// monitoring endpoint. Defaults to "core-nats" when empty.
	ConnectionName string

	// DialTimeout bounds the initial connect attempt. Defaults to 10s
	// when zero.
	DialTimeout time.Duration

	// ReconnectWait is the delay between reconnect attempts after a lost
	// connection. Defaults to 2s when zero.
	ReconnectWait time.Duration

	// MaxReconnects bounds the number of reconnect attempts. Pointer
	// semantics distinguish "unset" from explicit "0".
	//
	//   nil  → use the nats.go default (60 attempts).
	//   *v=0 → no reconnect attempts (give up on first failure).
	//   *v=-1 (or &MaxReconnectsForever) → retry forever.
	//   *v>0 → cap at v attempts.
	MaxReconnects *int
}

// validate returns an error if mandatory fields are missing.
func (c Config) validate() error {
	if c.URL == "" {
		return ErrEmptyURL
	}

	return nil
}

// connectNATS dials NATS using the given Config and applies the optional
// extra nats.Options after the Config-derived defaults so callers can
// override anything.
//
// Connection state callbacks (Disconnect/Reconnect/Closed) are wired to the
// given logger; pass a non-nil logger (the constructors default it).
func connectNATS(cfg Config, logger *slog.Logger, extra []nats.Option) (*nats.Conn, error) {
	if logger == nil {
		logger = slog.Default()
	}

	opts := baseConnectOptions(cfg, logger)
	opts = append(opts, authConnectOptions(cfg)...)
	opts = append(opts, extra...)

	nc, err := nats.Connect(cfg.URL, opts...)
	if err != nil {
		return nil, fmt.Errorf("connect to NATS URL %s: %w", cfg.URL, err)
	}

	return nc, nil
}

// baseConnectOptions returns name/timeout/reconnect/state-callback options
// derived from cfg. Defaults are substituted when a field is zero-valued.
func baseConnectOptions(cfg Config, logger *slog.Logger) []nats.Option {
	connName := cfg.ConnectionName
	if connName == "" {
		connName = defaultConnectionName
	}

	dialTimeout := cfg.DialTimeout
	if dialTimeout == 0 {
		dialTimeout = defaultDialTimeout
	}

	reconnectWait := cfg.ReconnectWait
	if reconnectWait == 0 {
		reconnectWait = defaultReconnectWait
	}

	opts := []nats.Option{
		nats.Name(connName),
		nats.Timeout(dialTimeout),
		nats.ReconnectWait(reconnectWait),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			logger.Warn("nats disconnected", slog.Any("err", err))
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			logger.Info("nats reconnected", slog.String("url", nc.ConnectedUrl()))
		}),
		nats.ClosedHandler(func(_ *nats.Conn) {
			logger.Info("nats connection closed")
		}),
		// Default async error handler — surfaces SlowConsumer and other
		// per-subscription errors that the NATS client would otherwise
		// drop silently. Callers can override via WithNATSOptions(nats.ErrorHandler(...)).
		nats.ErrorHandler(func(_ *nats.Conn, sub *nats.Subscription, err error) {
			subject := ""
			if sub != nil {
				subject = sub.Subject
			}

			logger.Warn("nats async error",
				slog.String("subject", subject),
				slog.Any("err", err),
			)
		}),
	}

	if cfg.MaxReconnects != nil {
		opts = append(opts, nats.MaxReconnects(*cfg.MaxReconnects))
	}

	return opts
}

// authConnectOptions appends authentication-related nats.Options based on
// which Config fields are populated. At most one of User+Password, Token,
// or CredsFile is expected to be set.
func authConnectOptions(cfg Config) []nats.Option {
	var opts []nats.Option

	if cfg.User != "" || cfg.Password != "" {
		opts = append(opts, nats.UserInfo(cfg.User, cfg.Password))
	}

	if cfg.Token != "" {
		opts = append(opts, nats.Token(cfg.Token))
	}

	if cfg.CredsFile != "" {
		opts = append(opts, nats.UserCredentials(cfg.CredsFile))
	}

	return opts
}

// acquireConn returns the connection to use and whether the caller owns it
// (and must close it on Shutdown). Shared by Publisher, Subscriber, and
// JetStreamConsumer constructors.
func acquireConn(cfg Config, common commonOptions) (*nats.Conn, bool, error) {
	if common.conn != nil {
		return common.conn, false, nil
	}

	if err := cfg.validate(); err != nil {
		return nil, false, err
	}

	nc, err := connectNATS(cfg, common.logger, common.extraNATS)
	if err != nil {
		return nil, false, fmt.Errorf("connect NATS: %w", err)
	}

	return nc, true, nil
}

// closeOwned closes nc if owned is true. Safe with nil nc.
func closeOwned(nc *nats.Conn, owned bool) {
	if owned && nc != nil {
		nc.Close()
	}
}
