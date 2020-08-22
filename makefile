all:
	PKG_CONFIG_PATH=/usr/local/share/pkgconfig go install
	date >>/tmp/barrier
clean:
	go clean
edit:
	vi cmd/gateway/gfarm/gateway-gfarm.go pkg/gfarm/gfarmClient.go
