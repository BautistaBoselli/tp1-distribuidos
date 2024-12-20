package main

import "time"

const (
	ElectionTimeout         = 2 * time.Second
	OkResponseTimeout       = 1000 * time.Millisecond
	PongTimeout             = 1200 * time.Millisecond
	PingToLeaderTimeout     = 400 * time.Millisecond
	ResurrecterPingTimeout  = 200 * time.Millisecond
	ResurrecterPingInterval = 400 * time.Millisecond
	ResurrecterRestartDelay = 2 * time.Second
	ResurrecterPingRetries  = 2
)
