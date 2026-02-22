package cmd

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gotd/td/session"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/dcs"
	"github.com/gotd/td/telegram/message"
	"github.com/gotd/td/telegram/uploader"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	_ "github.com/lib/pq"
)

const (
	numUploadWorkers    = 8   // Number of concurrent upload workers
	uploadQueueCapacity = 200 // Capacity of the upload job queue
	numDownloadClients  = 4   // Number of parallel Telegram clients for downloads
)

// uploadJob represents a single file chunk to be uploaded to Telegram.
type uploadJob struct {
	ctx        context.Context
	partName   string
	reader     io.Reader
	size       int64
	resultChan chan<- uploadResult
}

// uploadResult contains the result of an upload job.
type uploadResult struct {
	msgID int
	err   error
}

// TelegramObjectLayer implements ObjectLayer interface using Telegram as backend
type TelegramObjectLayer struct {
	ObjectLayer                    // Embeds ALL native MinIO methods automatically!
	base              ObjectLayer  // Reference to native layer for delegation
	tgClient          *telegram.Client
	downloadClients   []*telegram.Client // Pool of clients for parallel downloads
	dlClientIdx       atomic.Uint64      // Round-robin index for download clients
	db                *sql.DB
	config            *TelegramConfig
	channelAccessHash atomic.Int64 // Atomic for lock-free reads on hot path
	hashMu            sync.RWMutex // Only used to guard runCtx now
	readyCh           chan struct{}
	uploadQueue       chan uploadJob
	runCtx            context.Context
}

// nextDownloadAPI returns the next API client from the download pool in round-robin fashion.
func (t *TelegramObjectLayer) nextDownloadAPI() *tg.Client {
	if len(t.downloadClients) == 0 {
		return t.tgClient.API()
	}
	idx := t.dlClientIdx.Add(1) % uint64(len(t.downloadClients))
	return t.downloadClients[idx].API()
}

// uploadWorker is a background worker that processes upload jobs from the queue.
func (t *TelegramObjectLayer) uploadWorker() {
	u := uploader.NewUploader(t.tgClient.API()).WithThreads(16).WithPartSize(512 * 1024)
	sender := message.NewSender(t.tgClient.API()).WithUploader(u)

	t.hashMu.RLock()
	target := sender.To(&tg.InputPeerChannel{
		ChannelID:  t.config.BareChannelID,
		AccessHash: t.channelAccessHash.Load(),
	})
	t.hashMu.RUnlock()

	for job := range t.uploadQueue {
		func() {
			if closer, ok := job.reader.(io.Closer); ok {
				defer closer.Close()
			}

			t.hashMu.RLock()
			sendCtx := t.runCtx
			t.hashMu.RUnlock()
			if sendCtx == nil {
				sendCtx = job.ctx
			}

			var upload tg.InputFileClass
			var uploadErr error

			for retries := 0; retries < 15; retries++ {
				upload, uploadErr = u.FromReader(sendCtx, job.partName, job.reader)
				if uploadErr == nil {
					break
				}
				if d, ok := tgerr.AsFloodWait(uploadErr); ok {
					time.Sleep(d + time.Second)
					continue
				}
				if seeker, ok := job.reader.(io.Seeker); ok {
					seeker.Seek(0, io.SeekStart)
				}
				fmt.Printf("Worker upload failed for %s: %v. Retry %d/15...\n", job.partName, uploadErr, retries+1)
				time.Sleep(time.Duration(retries+1) * 200 * time.Millisecond)
			}

			if uploadErr != nil {
				job.resultChan <- uploadResult{err: fmt.Errorf("uploader failed after retries: %w", uploadErr)}
				return
			}

			var msgUpdates tg.UpdatesClass
			var lastErr error

			for retries := 0; retries < 15; retries++ {
				msgUpdates, lastErr = target.File(sendCtx, upload)
				if lastErr == nil {
					break
				}
				if d, ok := tgerr.AsFloodWait(lastErr); ok {
					time.Sleep(d + time.Second)
					continue
				}
				if retries < 5 {
					time.Sleep(time.Duration(retries+1) * 200 * time.Millisecond)
					continue
				}
				break
			}

			if lastErr != nil {
				job.resultChan <- uploadResult{err: fmt.Errorf("worker send failed after retries: %w", lastErr)}
				return
			}

			msgID := 0
			switch upds := msgUpdates.(type) {
			case *tg.Updates:
				for _, upd := range upds.Updates {
					switch update := upd.(type) {
					case *tg.UpdateNewMessage:
						if m, ok := update.Message.(*tg.Message); ok {
							msgID = m.ID
						}
					case *tg.UpdateNewChannelMessage:
						if m, ok := update.Message.(*tg.Message); ok {
							msgID = m.ID
						}
					}
					if msgID != 0 {
						break
					}
				}
			case *tg.UpdateShortSentMessage:
				msgID = upds.ID
			}

			if msgID == 0 {
				job.resultChan <- uploadResult{err: fmt.Errorf("worker failed to extract message ID")}
				return
			}

			job.resultChan <- uploadResult{msgID: msgID}
		}()
	}
}

func (t *TelegramObjectLayer) isSystemBucket(bucket string) bool {
	return bucket == minioMetaBucket || bucket == minioMetaMultipartBucket || bucket == ".minio.sys"
}

func (t *TelegramObjectLayer) tgReady(ctx context.Context) error {
	select {
	case <-t.readyCh:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("telegram client not ready: %w", ctx.Err())
	}
}

// newTelegramClient creates a new Telegram client with the given config and options.
func newTelegramClient(cfg *TelegramConfig, sessionPath string, proxyURL string) (*telegram.Client, error) {
	opts := telegram.Options{
		SessionStorage: &session.FileStorage{Path: sessionPath},
		NoUpdates:      true,
	}

	if proxyURL != "" {
		u, err := url.Parse(proxyURL)
		if err != nil {
			return nil, fmt.Errorf("failed to parse proxy URL: %w", err)
		}
		q := u.Query()
		server := q.Get("server")
		port := q.Get("port")
		secretHex := q.Get("secret")

		if server != "" && port != "" && secretHex != "" {
			secret, err := hex.DecodeString(secretHex)
			if err != nil {
				return nil, fmt.Errorf("invalid proxy secret: %w", err)
			}
			addr := net.JoinHostPort(server, port)
			resolver, err := dcs.MTProxy(addr, secret, dcs.MTProxyOptions{})
			if err != nil {
				return nil, fmt.Errorf("failed to create MTProxy resolver: %w", err)
			}
			opts.Resolver = resolver
		}
	}

	return telegram.NewClient(cfg.AppID, cfg.AppHash, opts), nil
}

// NewTelegramObjectLayer initializes the Telegram object wrapper
func NewTelegramObjectLayer(ctx context.Context, base ObjectLayer) (ObjectLayer, error) {
	cfg := LoadTelegramConfig()

	db, err := sql.Open("postgres", cfg.PostgresURL)
	if err != nil {
		return nil, fmt.Errorf("failed to open postgres connection: %w", err)
	}

	schema := `
	CREATE TABLE IF NOT EXISTS buckets (name TEXT PRIMARY KEY, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
	CREATE TABLE IF NOT EXISTS objects (bucket TEXT NOT NULL, key TEXT NOT NULL, metadata JSONB, data BYTEA, PRIMARY KEY (bucket, key));
	`
	db.ExecContext(ctx, schema)
	db.ExecContext(ctx, "ALTER TABLE objects ADD COLUMN IF NOT EXISTS data BYTEA")

	// Create the primary upload client
	primaryClient, err := newTelegramClient(cfg, "tg_session.json", cfg.ProxyURL)
	if err != nil {
		return nil, fmt.Errorf("failed to create primary telegram client: %w", err)
	}

	// Create dedicated download clients (each gets its own session file)
	downloadClients := make([]*telegram.Client, numDownloadClients)
	for i := 0; i < numDownloadClients; i++ {
		sessionPath := fmt.Sprintf("tg_session_dl%d.json", i)
		dlClient, err := newTelegramClient(cfg, sessionPath, cfg.ProxyURL)
		if err != nil {
			return nil, fmt.Errorf("failed to create download client %d: %w", i, err)
		}
		downloadClients[i] = dlClient
	}

	layer := &TelegramObjectLayer{
		ObjectLayer:     base,
		base:            base,
		db:              db,
		config:          cfg,
		tgClient:        primaryClient,
		downloadClients: downloadClients,
		readyCh:         make(chan struct{}),
		uploadQueue:     make(chan uploadJob, uploadQueueCapacity),
	}

	bgCtx := context.Background()
	var readyOnce sync.Once

	// Start all download clients in background
	for i, dlClient := range downloadClients {
		dlClient := dlClient
		clientIdx := i
		go func() {
			for {
				err := dlClient.Run(bgCtx, func(ctx context.Context) error {
					authStatus, err := dlClient.Auth().Status(ctx)
					if err != nil {
						return err
					}
					if !authStatus.Authorized {
						if _, authErr := dlClient.Auth().Bot(ctx, cfg.BotToken); authErr != nil {
							return authErr
						}
					}
					
					// Warm up download client session by fetching dialogs or the channel
					// This prevents CHANNEL_INVALID errors if they ever need to use channel-scoped methods
					api := dlClient.API()
					api.MessagesGetDialogs(ctx, &tg.MessagesGetDialogsRequest{Limit: 1})
					api.ChannelsGetChannels(ctx, []tg.InputChannelClass{
						&tg.InputChannel{ChannelID: cfg.BareChannelID, AccessHash: 0},
					})

					fmt.Printf("Download client %d connected and warmed up\n", clientIdx)
					<-ctx.Done()
					return ctx.Err()
				})
				fmt.Printf("Download client %d exited: %v. Reconnecting in 3s...\n", clientIdx, err)
				time.Sleep(3 * time.Second)
			}
		}()
	}

	// Start primary client
	go func() {
		for {
			err := primaryClient.Run(bgCtx, func(ctx context.Context) error {
				authStatus, err := primaryClient.Auth().Status(ctx)
				if err != nil {
					return fmt.Errorf("failed to check auth status: %w", err)
				}
				if !authStatus.Authorized {
					if _, authErr := primaryClient.Auth().Bot(ctx, cfg.BotToken); authErr != nil {
						fmt.Printf("Fatal: Telegram Bot Auth failed: %v\n", authErr)
						return authErr
					}
				}

				api := primaryClient.API()
				var hash int64

				// Strategy 1: Direct fetch (works if already known to session)
				chResult, _ := api.ChannelsGetChannels(ctx, []tg.InputChannelClass{
					&tg.InputChannel{ChannelID: cfg.BareChannelID, AccessHash: 0},
				})
				if chats, ok := chResult.(*tg.MessagesChats); ok && len(chats.Chats) > 0 {
					for _, chat := range chats.Chats {
						if c, ok := chat.(*tg.Channel); ok && c.ID == cfg.BareChannelID {
							hash = c.AccessHash
							break
						}
					}
				}

				// Strategy 2: Dialogs (more reliable for Bots to discover channels)
				if hash == 0 {
					dialogs, _ := api.MessagesGetDialogs(ctx, &tg.MessagesGetDialogsRequest{
						Limit: 100,
					})
					if d, ok := dialogs.(tg.MessagesDialogsClass); ok {
						var chats []tg.ChatClass
						switch v := d.(type) {
						case *tg.MessagesDialogs:
							chats = v.Chats
						case *tg.MessagesDialogsSlice:
							chats = v.Chats
						}
						for _, chat := range chats {
							if c, ok := chat.(*tg.Channel); ok && c.ID == cfg.BareChannelID {
								hash = c.AccessHash
								break
							}
						}
					}
				}

				if hash != 0 {
					layer.channelAccessHash.Store(hash)
					fmt.Printf("Telegram channelAccessHash resolved: %d\n", hash)
				} else {
					fmt.Printf("Warning: Failed to resolve channelAccessHash for ID %d. Retrying in loop...\n", cfg.BareChannelID)
					time.Sleep(2 * time.Second)
					return fmt.Errorf("channel hash not yet resolved")
				}

				layer.hashMu.Lock()
				layer.runCtx = ctx
				layer.hashMu.Unlock()

				readyOnce.Do(func() {
					close(layer.readyCh)
					for i := 0; i < numUploadWorkers; i++ {
						go layer.uploadWorker()
					}
				})

				<-ctx.Done()
				return ctx.Err()
			})
			fmt.Printf("TELEGRAM PRIMARY CLIENT RUN EXITED with error: %v. Reconnecting in 3s...\n", err)
			time.Sleep(3 * time.Second)
		}
	}()

	setObjectLayer(layer)
	return layer, nil
}