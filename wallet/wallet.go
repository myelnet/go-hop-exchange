package wallet

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/crypto"
	keystore "github.com/ipfs/go-ipfs-keystore"
	ci "github.com/libp2p/go-libp2p-core/crypto"
	pb "github.com/libp2p/go-libp2p-core/crypto/pb"
	fil "github.com/myelnet/pop/filecoin"
)

var ErrNoAPI = fmt.Errorf("no filecoin api connected")

const (
	KNamePrefix = "wallet-"
	KDefault    = "default"
)

// KeyType enumerates all the types of key we support
type KeyType string

// Only supporting secp for now since bls cannot be stored in ipfs keystore
const (
	KTSecp256k1 KeyType = "secp256k1"
	KTBLS       KeyType = "bls"
)

func init() {
	address.CurrentNetwork = address.Mainnet
}

// KeyInfo stores info about a private key
type KeyInfo struct {
	KType      KeyType `json:"Type"` // Had to name it KType as Type() is used already
	PrivateKey []byte
	//internal signer
	sig Signer
}

func (k *KeyInfo) FromBytes(data []byte) error {
	return json.Unmarshal(data, k)
}

func (k *KeyInfo) ToBytes() ([]byte, error) {
	return json.Marshal(k)
}

// Key adds the public key and address on top of the private key
type Key struct {
	KeyInfo

	PublicKey []byte
	Address   address.Address
}

// Bytes implements libp2p PrivKey interface for IPFS keystore compat
// do not use
func (k *Key) Bytes() ([]byte, error) {
	return k.PrivateKey, nil
}

// Raw implements libp2p Key interface for IPFS keystore compat
func (k *Key) Raw() ([]byte, error) {
	return k.PrivateKey, nil
}

// Equals implements libp2p key interface for IPFS keystore compat
func (k *Key) Equals(ki ci.Key) bool {
	pki, _ := ki.Raw()
	return bytes.Equal(k.PrivateKey, pki)
}

// Type implements libp2p key interface for IPFS keystore compat
func (k *Key) Type() pb.KeyType {
	switch k.KType {
	case KTSecp256k1:
		return pb.KeyType_Secp256k1
	case KTBLS:
		return pb.KeyType(4) // 4 doesnt' exist in libp2p
	default:
		// No unknown so we fall back to RSA as our unknown
		return pb.KeyType_RSA
	}
}

// Sign implements libp2p PrivKey interface for IPFS keystore compat
func (k *Key) Sign(data []byte) ([]byte, error) {
	return k.sig.Sign(k.PrivateKey, data)
}

// GetPublic implements libp2p PrivKey interface for IPFS keystore compat
func (k *Key) GetPublic() ci.PubKey {
	return k
}

// Verify implements libp2p PubKey interface for IPFS keystore compat
func (k *Key) Verify(data []byte, sig []byte) (bool, error) {
	err := k.sig.Verify(sig, k.Address, data)
	if err != nil {
		return false, err
	}
	return true, nil
}

// NewKeyFromKeyInfo adds public key and address to private key
func NewKeyFromKeyInfo(ki KeyInfo) (*Key, error) {
	var err error
	k := &Key{
		KeyInfo: ki,
	}
	k.PublicKey, err = ki.sig.ToPublic(ki.PrivateKey)
	if err != nil {
		return nil, err
	}

	switch ki.KType {
	case KTSecp256k1:
		k.Address, err = address.NewSecp256k1Address(k.PublicKey)
		if err != nil {
			return nil, err
		}
	case KTBLS:
		k.Address, err = address.NewBLSAddress(k.PublicKey)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("key type not supported")
	}
	return k, nil
}

// NewKeyFromLibp2p converts a libp2p crypto private key interface into a Key
func NewKeyFromLibp2p(pk ci.PrivKey, sigs map[KeyType]Signer) (*Key, error) {
	var tp KeyType
	switch pk.Type() {
	case pb.KeyType_Secp256k1:
		tp = KTSecp256k1
	case 4:
		tp = KTBLS
	default:
		return nil, fmt.Errorf("key type not supported")
	}
	raw, err := pk.Raw()
	if err != nil {
		return nil, err
	}
	sig, err := KeyTypeSig(tp, sigs)
	if err != nil {
		return nil, err
	}
	// Hopefully we don't get an error?
	ki := KeyInfo{
		KType:      tp,
		PrivateKey: raw,
		sig:        sig,
	}
	return NewKeyFromKeyInfo(ki)
}

// Signer encapsulates methods for a given key
type Signer interface {
	GenPrivate() ([]byte, error)
	ToPublic(pk []byte) ([]byte, error)
	Sign(pk []byte, msg []byte) ([]byte, error)
	Verify(sig []byte, a address.Address, msg []byte) error
}

//Driver is a lightweight interface to control any available keychain and interact with blockchains
type Driver interface {
	NewKey(context.Context, KeyType) (address.Address, error)
	DefaultAddress() address.Address
	SetDefaultAddress(address.Address) error
	List() ([]address.Address, error)
	ImportKey(context.Context, *KeyInfo) (address.Address, error)
	ExportKey(context.Context, address.Address) (*KeyInfo, error)
	Sign(context.Context, address.Address, []byte) (*crypto.Signature, error)
	Verify(context.Context, address.Address, []byte, *crypto.Signature) (bool, error)
	Balance(context.Context, address.Address) (fil.BigInt, error)
	Transfer(ctx context.Context, from address.Address, to address.Address, amount string) error
	Signers() map[KeyType]Signer
}

// KeystoreWallet wraps an IPFS keystore
type KeystoreWallet struct {
	keystore keystore.Keystore
	// API to interact with Filecoin chain
	fAPI fil.API
	sigs map[KeyType]Signer

	mu          sync.Mutex
	keys        map[address.Address]*Key // cache so we don't read from the Keystore too much
	defaultAddr address.Address
}

// Option is an optional configuration of the wallet
type Option func(kw *KeystoreWallet)

// WithBLSSig adds a signer for the BLS key type
func WithBLSSig(sig Signer) Option {
	return func(kw *KeystoreWallet) {
		kw.sigs[KTBLS] = sig
	}
}

// WithFilAPI sets the filecoin API client
func WithFilAPI(f fil.API) Option {
	return func(kw *KeystoreWallet) {
		kw.fAPI = f
	}
}

// NewFromKeystore creates a new IPFS keystore based wallet implementing the Driver methods
func NewFromKeystore(ks keystore.Keystore, opts ...Option) Driver {
	w := &KeystoreWallet{
		keystore:    ks,
		keys:        make(map[address.Address]*Key),
		sigs:        make(map[KeyType]Signer),
		defaultAddr: address.Undef,
	}
	// Add Secp256k1 by default
	w.sigs[KTSecp256k1] = secp{}

	for _, opt := range opts {
		opt(w)
	}

	// cache the default address if we have any
	defAddr, err := w.getDefaultAddress()
	if err != nil {
		return w
	}

	w.defaultAddr = defAddr
	return w
}

// NewKey generates a brand new key for the given type in our wallet and returns the address
func (w *KeystoreWallet) NewKey(ctx context.Context, kt KeyType) (address.Address, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	sig, err := KeyTypeSig(kt, w.sigs)
	if err != nil {
		return address.Undef, err
	}
	pk, err := sig.GenPrivate()
	if err != nil {
		return address.Undef, err
	}
	ki := KeyInfo{
		KType:      kt,
		PrivateKey: pk,
		sig:        sig,
	}

	k, err := NewKeyFromKeyInfo(ki)
	if err != nil {
		return address.Undef, err
	}

	if err := w.keystore.Put(KNamePrefix+k.Address.String(), k); err != nil {
		return address.Undef, fmt.Errorf("unable to save to keystore: %v", err)
	}
	w.keys[k.Address] = k

	if w.defaultAddr == address.Undef {
		if err := w.keystore.Put(KDefault, k); err != nil {
			return address.Undef, fmt.Errorf("failed to set new key as default: %v", err)
		}
		w.defaultAddr = k.Address
	}
	return k.Address, nil
}

// read default address from keystore
func (w *KeystoreWallet) getDefaultAddress() (address.Address, error) {
	k, err := w.keystore.Get(KDefault)
	if err != nil {
		return address.Undef, err
	}

	key, err := NewKeyFromLibp2p(k, w.sigs)
	if err != nil {
		return address.Undef, err
	}
	return key.Address, nil
}

// DefaultAddress of the wallet used for receiving payments as provider and paying when retrieving
func (w *KeystoreWallet) DefaultAddress() address.Address {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.defaultAddr
}

// SetDefaultAddress for receiving and sending payments as provider or client
func (w *KeystoreWallet) SetDefaultAddress(addr address.Address) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	k, err := w.keystore.Get(KNamePrefix + addr.String())
	if err != nil {
		return err
	}

	_ = w.keystore.Delete(KDefault)

	if err := w.keystore.Put(KDefault, k); err != nil {
		return err
	}
	w.defaultAddr = addr

	return nil
}

// List all the addresses in the wallet
func (w *KeystoreWallet) List() ([]address.Address, error) {
	all, err := w.keystore.List()
	if err != nil {
		return nil, err
	}

	sort.Strings(all)

	added := make(map[address.Address]bool)
	var list []address.Address
	for _, a := range all {
		if strings.HasPrefix(a, KNamePrefix) {
			name := strings.TrimPrefix(a, KNamePrefix)
			addr, err := address.NewFromString(name)
			if err != nil {
				return nil, err
			}
			if _, ok := added[addr]; ok {
				continue
			}
			added[addr] = true
			list = append(list, addr)
		}
	}

	return list, nil
}

// ImportKey in the wallet from a private key and key type
func (w *KeystoreWallet) ImportKey(_ context.Context, k *KeyInfo) (address.Address, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	sig, err := KeyTypeSig(k.KType, w.sigs)
	if err != nil {
		return address.Undef, err
	}

	k.sig = sig
	key, err := NewKeyFromKeyInfo(*k)
	if err != nil {
		return address.Undef, err
	}

	if err := w.keystore.Put(KNamePrefix+key.Address.String(), key); err != nil {
		return address.Undef, err
	}
	w.keys[key.Address] = key
	return key.Address, nil
}

// ExportKey to a private key and key type from an address in the wallet
func (w *KeystoreWallet) ExportKey(_ context.Context, addr address.Address) (*KeyInfo, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	key, exists := w.keys[addr]
	if !exists {
		return nil, fmt.Errorf("address %s does not exist", addr.String())
	}

	return &key.KeyInfo, nil
}

// Sign a message with the key associated with the given address. Generates a valid Filecoin signature
func (w *KeystoreWallet) Sign(ctx context.Context, addr address.Address, msg []byte) (*crypto.Signature, error) {
	k, err := w.getKey(addr)
	if err != nil {
		return nil, err
	}
	sigType := ActSigType(k.KType)
	s, err := k.sig.Sign(k.PrivateKey, msg)
	if err != nil {
		return nil, err
	}
	return &crypto.Signature{
		Type: sigType,
		Data: s,
	}, nil
}

// Verify a signature for the given bytes
func (w *KeystoreWallet) Verify(ctx context.Context, k address.Address, msg []byte, sig *crypto.Signature) (bool, error) {
	signer, err := SigTypeSig(sig.Type, w.sigs)
	if err != nil {
		return false, err
	}
	err = signer.Verify(sig.Data, k, msg)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (w *KeystoreWallet) getKey(addr address.Address) (*Key, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	k, ok := w.keys[addr]
	if ok {
		return k, nil
	}
	ki, err := w.keystore.Get(KNamePrefix + addr.String())
	if err != nil {
		return nil, err
	}

	k, err = NewKeyFromLibp2p(ki, w.sigs)
	if err != nil {
		return nil, err
	}

	w.keys[k.Address] = k
	return k, nil
}

// Signers returns all the signers registered for this wallet
func (w *KeystoreWallet) Signers() map[KeyType]Signer {
	return w.sigs
}

// KeyTypeSig selects the signer based on key type
func KeyTypeSig(typ KeyType, sigs map[KeyType]Signer) (Signer, error) {
	sig, ok := sigs[typ]
	if !ok {
		return nil, fmt.Errorf("key type not supported: %s", typ)
	}
	return sig, nil
}

// SigTypeSig selects the signer based on sig type
func SigTypeSig(st crypto.SigType, sigs map[KeyType]Signer) (Signer, error) {
	switch st {
	case crypto.SigTypeSecp256k1:
		return KeyTypeSig(KTSecp256k1, sigs)
	case crypto.SigTypeBLS:
		return KeyTypeSig(KTBLS, sigs)
	default:
		return nil, fmt.Errorf("sig type not supported")
	}
}

// ActSigType converts a key type to a Filecoin signature type
func ActSigType(typ KeyType) crypto.SigType {
	switch typ {
	case KTBLS:
		return crypto.SigTypeBLS
	case KTSecp256k1:
		return crypto.SigTypeSecp256k1
	default:
		return crypto.SigTypeUnknown
	}
}

// ------------------ Filecoin Methods -------------------------

// Balance for a given address
func (w *KeystoreWallet) Balance(ctx context.Context, addr address.Address) (fil.BigInt, error) {
	if w.fAPI == nil {
		return big.Zero(), ErrNoAPI
	}
	state, err := w.fAPI.StateReadState(ctx, addr, fil.EmptyTSK)
	if err != nil || state == nil {
		return big.Zero(), err
	}
	return state.Balance, nil
}

// Transfer from an address in our wallet to any given address
// FIL amount is passed as a human readable string
// this methods blocks execution until the transaction was seen on chain
func (w *KeystoreWallet) Transfer(ctx context.Context, from address.Address, to address.Address, amount string) error {
	if w.fAPI == nil {
		return ErrNoAPI
	}
	// Immediately fail if we don't have the address to avoid unnecessary requests
	if _, err := w.getKey(from); err != nil {
		return err
	}

	val, err := fil.ParseFIL(amount)
	if err != nil {
		return err
	}

	method := abi.MethodNum(uint64(0))
	msg := &fil.Message{
		From:   from,
		To:     to,
		Value:  fil.BigInt(val),
		Method: method,
	}

	msg, err = w.fAPI.GasEstimateMessageGas(ctx, msg, nil, fil.EmptyTSK)
	if err != nil {
		return err
	}

	act, err := w.fAPI.StateGetActor(ctx, msg.From, fil.EmptyTSK)
	if err != nil {
		return err
	}
	msg.Nonce = act.Nonce

	mbl, err := msg.ToStorageBlock()
	if err != nil {
		return err
	}

	sig, err := w.Sign(ctx, msg.From, mbl.Cid().Bytes())
	if err != nil {
		return err
	}

	smsg := &fil.SignedMessage{
		Message:   *msg,
		Signature: *sig,
	}

	if _, err := w.fAPI.MpoolPush(ctx, smsg); err != nil {
		return fmt.Errorf("MpoolPush failed with error: %v", err)
	}

	mwait, err := w.fAPI.StateWaitMsg(ctx, smsg.Cid(), uint64(5))
	if err != nil {
		return fmt.Errorf("Failed to wait for msg: %s", err)
	}

	if mwait.Receipt.ExitCode != 0 {
		return fmt.Errorf("Tx failed (exit code %d)", mwait.Receipt.ExitCode)
	}

	return nil
}
