all:
	PKG_CONFIG_PATH=/usr/local/share/pkgconfig go install
edit:
	vi cmd/gateway/gfarm/gateway-gfarm.go pkg/gfarm/gfarmClient.go
