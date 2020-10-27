#! /bin/sh

perl cmd/gateway/gfarm/msgno/assign_msgno.pl \
	$(find cmd/gateway/gfarm -type f ! -name gfarmClientMsgEnums.go -name '*.go')
