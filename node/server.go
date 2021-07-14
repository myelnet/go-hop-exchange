package node

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net"
	"net/http"
	gopath "path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/filecoin-project/go-jsonrpc"
	"github.com/gabriel-vasile/mimetype"
	"github.com/ipfs/go-cid"
	files "github.com/ipfs/go-ipfs-files"
	ipath "github.com/ipfs/go-path"
	"github.com/jpillora/backoff"
	"github.com/myelnet/pop/exchange"
	"github.com/myelnet/pop/internal/utils"
	sel "github.com/myelnet/pop/selectors"
	"github.com/rs/zerolog/log"
	"github.com/soheilhy/cmux"
	"runtime/debug"
)

// server listens for connection and controls the node to execute requests
type server struct {
	node *node

	csMu sync.Mutex // lock order: csMu, then mu
	cs   *CommandServer

	mu      sync.Mutex
	clients map[net.Conn]bool
}

func (s *server) serveConn(ctx context.Context, c net.Conn) {
	br := bufio.NewReader(c)
	c.SetReadDeadline(time.Time{})

	s.addConn(c)
	defer s.removeAndCloseConn(c)

	for ctx.Err() == nil {
		msg, err := ReadMsg(br)
		if errors.Is(err, io.EOF) {
			return
		}
		if err != nil {
			log.Error().Err(err).Msg("ReadMsg")
			return
		}
		s.csMu.Lock()
		if err := s.cs.GotMsgBytes(ctx, msg); err != nil {
			log.Error().Err(err).Msg("GotMsgBytes")
		}
		s.csMu.Unlock()
	}
}

func (s *server) addConn(c net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.clients == nil {
		s.clients = map[net.Conn]bool{}
	}

	s.clients[c] = true
}

func (s *server) removeAndCloseConn(c net.Conn) {
	s.mu.Lock()
	delete(s.clients, c)
	s.mu.Unlock()
	c.Close()
}

func (s *server) writeToClients(b []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for c := range s.clients {
		WriteMsg(c, b)
	}
}

func (s *server) localhostHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" && r.Method == http.MethodGet {
			io.WriteString(w, "<html><title>pop</title><body><h1>Hello</h1>This is your Myel pop.")
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), time.Hour)
		defer cancel()
		r = r.WithContext(ctx)

		switch r.Method {
		case http.MethodGet, http.MethodHead:
			s.getHandler(w, r)
			return
		case http.MethodOptions:
			s.optionsHandler(w, r)
			return
		case http.MethodPost:
			s.postHandler(w, r)
			return
		}

		errmsg := "Method " + r.Method + " not allowed: "
		status := http.StatusBadRequest
		errmsg = errmsg + "bad request for " + r.URL.Path
		http.Error(w, errmsg, status)
	})
}

func (s *server) optionsHandler(w http.ResponseWriter, r *http.Request) {
	s.addUserHeaders(w)
}

func (s *server) addUserHeaders(w http.ResponseWriter) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header()["Access-Control-Allow-Methods"] = []string{http.MethodPost, http.MethodGet}
	w.Header()["Access-Control-Allow-Headers"] = []string{"Content-Type", "User-Agent", "Range"}
	w.Header()["Access-Control-Expose-Headers"] = []string{"IPFS-Hash"}
}

// HTTP get does not retrieve content but only serves content already cached locally or for which a loaded
// paychannel already exists to make sure content is loaded use JSON RPC method Load available via websocket
func (s *server) getHandler(w http.ResponseWriter, r *http.Request) {
	urlPath := r.URL.Path

	parsedPath := ipath.FromString(urlPath)

	// Extract the CID and file path segments
	root, segs, err := ipath.SplitAbsPath(parsedPath)
	if err != nil {
		http.Error(w, "invalid path", http.StatusBadRequest)
		return
	}
	var key string
	if len(segs) > 0 {
		key = segs[0]
	}

	s.addUserHeaders(w)

	tx := s.node.exch.Tx(r.Context(), exchange.WithRoot(root))

	has := tx.IsLocal(key)
	if !has {
		// If there is already a payment channel open we can handle it
		// else the delay for loading a payment channel is not reasonnable for an HTTP request
		_, err = s.node.omg.GetOffer(root)
		if err != nil {
			http.Error(w, "content not cached on this node", http.StatusNotFound)
			return
		}
		results, err := s.node.Load(r.Context(), &GetArgs{Cid: urlPath})
		if err != nil {
			http.Error(w, "failed to load", http.StatusInternalServerError)
			return
		}
		for range results {
		}
	}

	if key == "" {
		// If there is no key we return all the entries as a JSON file detailing information
		// about each entry. This allows clients to inspec the content in a transaction before
		// fetching all of it.
		entries, err := tx.Entries()
		if err != nil {
			http.Error(w, "Failed to get entries", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(entries)
		return
	}
	fnd, err := tx.GetFile(segs[0])
	if err != nil {
		http.Error(w, "Failed to read file from store", http.StatusInternalServerError)
		return
	}

	modtime := time.Now()
	if f, ok := fnd.(files.File); ok {
		name := gopath.Base(urlPath)

		size, err := f.Size()
		if err != nil {
			http.Error(w, "cannot serve files with unknown sizes", http.StatusBadGateway)
			return
		}
		content := &lazySeeker{
			size:   size,
			reader: f,
		}

		mimeType, err := mimetype.DetectReader(content)
		if err != nil {
			http.Error(w, fmt.Sprintf("cannot detect content-type: %s", err.Error()), http.StatusInternalServerError)
			return
		}

		ctype := mimeType.String()
		_, err = content.Seek(0, io.SeekStart)
		if err != nil {
			http.Error(w, "seeker can't seek", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", ctype)
		http.ServeContent(w, r, name, modtime, content)
	}
}

func (s *server) postHandler(w http.ResponseWriter, r *http.Request) {
	mediatype, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil {
		http.Error(w, "unable to parse content type", http.StatusInternalServerError)
		return
	}

	var root cid.Cid
	if mediatype == "multipart/form-data" {
		mr := multipart.NewReader(r.Body, params["boundary"])
		tx := s.node.exch.Tx(r.Context())
		defer tx.Close()

		// Set Cache Replication-Factor
		cacheRF, err := parseContentReplication(r.Header.Get("Content-Replication"))
		if err != nil {
			http.Error(w, "failed to parse Content-Replication", http.StatusInternalServerError)
			return
		}
		if cacheRF > 12 {
			cacheRF = 12
		}
		s.node.tx.SetCacheRF(cacheRF)

		for part, err := mr.NextPart(); err == nil; part, err = mr.NextPart() {
			c, err := s.node.Add(r.Context(), tx.Store().DAG, part)
			if err != nil {
				http.Error(w, "failed to add file", http.StatusInternalServerError)
				return
			}
			stats, err := utils.Stat(r.Context(), tx.Store(), c, sel.All())
			if err != nil {
				http.Error(w, "failed to get file stat", http.StatusInternalServerError)
				return
			}
			key := part.FileName()
			if key == "" {
				// If it's not a file the key should be the form name
				key = part.FormName()
			}
			err = tx.Put(key, c, int64(stats.Size))
			if err != nil {
				http.Error(w, "failed to put file in tx", http.StatusInternalServerError)
				return
			}
		}
		err = tx.Commit()
		if err != nil {
			http.Error(w, "failed to commit tx", http.StatusInternalServerError)
			return
		}
		err = s.node.exch.Index().SetRef(tx.Ref())
		if err != nil {
			http.Error(w, "failed to set new ref", http.StatusInternalServerError)
			return
		}
		root = tx.Root()
	} else {
		c, err := s.node.Add(r.Context(), s.node.dag, files.NewReaderFile(r.Body))
		if err != nil {
			http.Error(w, "failed to add file to blockstore", http.StatusInternalServerError)
			return
		}
		root = c
	}

	s.addUserHeaders(w)
	w.Header().Set("IPFS-Hash", root.String())
	http.Redirect(w, r, root.String(), http.StatusCreated)
}

func parseContentReplication(contentReplication string) (int, error) {
	if contentReplication == "" {
		return 0, nil
	}

	contentReplicationInt, err := strconv.ParseInt(contentReplication, 10, 64)
	if err != nil {
		return 0, errors.New("unable to parse content replication")
	}

	return int(contentReplicationInt), nil
}

// Run runs a pop IPFS node
func Run(ctx context.Context, opts Options) error {
	l, err := net.Listen("tcp", "127.0.0.1:2001")
	if err != nil {
		return fmt.Errorf("SocketListen: %v", err)
	}

	// Create a mux to listen for TCP / HTTP requests on same port
	m := cmux.New(l)
	defer m.Close()

	httpl := m.Match(cmux.HTTP1Fast())
	tcpl := m.Match(cmux.Any())

	nd, err := New(ctx, opts)
	if err != nil {
		return fmt.Errorf("node.New: %v", err)
	}

	fmt.Printf("==> Started pop node\n")
	fmt.Printf("==> Joined %s regions\n", opts.Regions)
	if nd.exch.IsFilecoinOnline() {
		fmt.Printf("==> Connected to Filecoin RPC at %s\n", opts.FilEndpoint)
	}

	server := &server{
		node: nd,
	}

	server.cs = NewCommandServer(nd, server.writeToClients)

	nd.notify = server.cs.send

	go serveHTTP(server, httpl)
	go serveTCP(ctx, server, tcpl)
	go func() {
		err := m.Serve()
		if err != nil && !strings.Contains(err.Error(), "use of closed network connection") {
			log.Error().Err(err).Msg("serve CMux")
		}
	}()

	<-ctx.Done()

	return ctx.Err()
}

func serveHTTP(server *server, l net.Listener) {
	mx := http.NewServeMux()

	// register RPC
	rpcServer := jsonrpc.NewServer()
	rpcServer.Register("pop", server.node)

	mx.Handle("/rpc", rpcServer)
	mx.Handle("/", server.localhostHandler())

	s := &http.Server{
		Handler: mx,
	}
	err := s.Serve(l)
	if err != nil && err != cmux.ErrServerClosed {
		log.Error().Err(err).Msg("serveHTTP")
	}
}

func serveTCP(ctx context.Context, server *server, l net.Listener) {
	b := backoff.Backoff{
		Min: time.Second,
		Max: time.Second * 5,
	}

	for ctx.Err() == nil {
		c, err := l.Accept()
		if err != nil {
			if ctx.Err() == nil {
				log.Error().Err(err).Msg("listen.Accept")

				// backOff
				delay := b.Duration()
				time.Sleep(delay)
			}
			continue
		}

		// reset backoff
		b.Reset()

		go func() {
			defer func() {
				if r := recover(); r != nil {
					log.Error().Msgf("recovered from panic : [%v] - stack trace : \n [%s]", r, debug.Stack())
				}
			}()

			server.serveConn(ctx, c)
		}()
	}
}
