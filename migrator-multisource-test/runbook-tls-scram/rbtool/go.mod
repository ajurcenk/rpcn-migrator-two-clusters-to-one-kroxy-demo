module rbtool

go 1.26

// Same franz-go versions as Redpanda Connect v4.100.0 (the migrator), like tests/go.mod.
require (
	github.com/twmb/franz-go v1.20.7
	github.com/twmb/franz-go/pkg/kadm v1.17.2
	github.com/twmb/franz-go/pkg/kmsg v1.12.0 // indirect
)

require (
	github.com/klauspost/compress v1.18.4 // indirect
	github.com/pierrec/lz4/v4 v4.1.25 // indirect
	golang.org/x/crypto v0.48.0 // indirect
)
