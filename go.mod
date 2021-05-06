module github.com/myelnet/pop

go 1.16

require (
	github.com/AlecAivazis/survey/v2 v2.2.9
	github.com/cpuguy83/go-md2man/v2 v2.0.0 // indirect
	github.com/docker/go-units v0.4.0
	github.com/filecoin-project/go-address v0.0.5-0.20201103152444-f2023ef3f5bb
	github.com/filecoin-project/go-amt-ipld/v2 v2.1.1-0.20201006184820-924ee87a1349 // indirect
	github.com/filecoin-project/go-bitfield v0.2.3-0.20201110211213-fe2c1862e816 // indirect
	github.com/filecoin-project/go-cbor-util v0.0.0-20201016124514-d0bbec7bfcc4
	github.com/filecoin-project/go-commp-utils v0.0.0-20201119054358-b88f7a96a434
	github.com/filecoin-project/go-crypto v0.0.0-20191218222705-effae4ea9f03
	github.com/filecoin-project/go-data-transfer v1.4.3
	github.com/filecoin-project/go-fil-markets v1.2.5
	github.com/filecoin-project/go-hamt-ipld/v3 v3.0.0
	github.com/filecoin-project/go-jsonrpc v0.1.2
	github.com/filecoin-project/go-multistore v0.0.3
	github.com/filecoin-project/go-state-types v0.0.0-20210119062722-4adba5aaea71
	github.com/filecoin-project/go-statemachine v0.0.0-20200925024713-05bd7c71fbfe
	github.com/filecoin-project/specs-actors/v3 v3.0.0
	github.com/gabriel-vasile/mimetype v1.1.2
	github.com/google/uuid v1.1.2
	github.com/hannahhoward/cbor-gen-for v0.0.0-20200817222906-ea96cece81f1
	github.com/hannahhoward/go-pubsub v0.0.0-20200423002714-8d62886cc36e
	github.com/ipfs/go-bitswap v0.3.2 // indirect
	github.com/ipfs/go-block-format v0.0.3
	github.com/ipfs/go-blockservice v0.1.4
	github.com/ipfs/go-cid v0.0.7
	github.com/ipfs/go-datastore v0.4.5
	github.com/ipfs/go-ds-badger v0.2.6
	github.com/ipfs/go-graphsync v0.7.0
	github.com/ipfs/go-ipfs-blockstore v1.0.3
	github.com/ipfs/go-ipfs-blocksutil v0.0.1
	github.com/ipfs/go-ipfs-chunker v0.0.5
	github.com/ipfs/go-ipfs-exchange-offline v0.0.1
	github.com/ipfs/go-ipfs-files v0.0.8
	github.com/ipfs/go-ipfs-keystore v0.0.2
	github.com/ipfs/go-ipld-cbor v0.0.5
	github.com/ipfs/go-ipld-format v0.2.0
	github.com/ipfs/go-merkledag v0.3.2
	github.com/ipfs/go-path v0.0.9
	github.com/ipfs/go-unixfs v0.2.4
	github.com/ipld/go-car v0.1.1-0.20201119040415-11b6074b6d4d
	github.com/ipld/go-ipld-prime v0.7.0
	github.com/ipld/go-ipld-prime-proto v0.1.1
	github.com/jpillora/backoff v1.0.0
	github.com/libp2p/go-eventbus v0.2.1
	github.com/libp2p/go-libp2p v0.13.0
	github.com/libp2p/go-libp2p-blankhost v0.2.0
	github.com/libp2p/go-libp2p-connmgr v0.2.4
	github.com/libp2p/go-libp2p-core v0.8.5
	github.com/libp2p/go-libp2p-kad-dht v0.11.1
	github.com/libp2p/go-libp2p-noise v0.1.2 // indirect
	github.com/libp2p/go-libp2p-peer v0.2.0
	github.com/libp2p/go-libp2p-peerstore v0.2.6
	github.com/libp2p/go-libp2p-pubsub v0.4.1
	github.com/libp2p/go-libp2p-swarm v0.4.0
	github.com/libp2p/go-libp2p-testing v0.4.0
	github.com/minio/blake2b-simd v0.0.0-20160723061019-3f5f724cb5b1
	github.com/multiformats/go-multiaddr v0.3.1
	github.com/multiformats/go-multihash v0.0.14
	github.com/onsi/ginkgo v1.14.0 // indirect
	github.com/peterbourgon/ff/v2 v2.0.0
	github.com/prometheus/common v0.10.0
	github.com/rs/zerolog v1.20.0
	github.com/sirupsen/logrus v1.6.0 // indirect
	github.com/stretchr/testify v1.6.1
	github.com/supranational/blst v0.3.2
	github.com/tchardin/go-libp2p-blankhost v0.2.1-0.20210408134851-9396bc83e200
	github.com/urfave/cli/v2 v2.2.0 // indirect
	github.com/whyrusleeping/cbor-gen v0.0.0-20210219115102-f37d292932f2
	github.com/xorcare/golden v0.6.1-0.20191112154924-b87f686d7542 // indirect
	go.opencensus.io v0.22.5 // indirect
	go.uber.org/multierr v1.6.0 // indirect
	golang.org/x/crypto v0.0.0-20201221181555-eec23a3978ad // indirect
	golang.org/x/exp v0.0.0-20200513190911-00229845015e // indirect
	golang.org/x/lint v0.0.0-20200302205851-738671d3881b // indirect
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1
	gonum.org/v1/gonum v0.9.1
)

replace github.com/filecoin-project/filecoin-ffi => ./extern/filecoin-ffi
