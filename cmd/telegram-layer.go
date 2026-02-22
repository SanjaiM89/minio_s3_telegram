package cmd

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/url"
	"sync"
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
	numUploadWorkers    = 8   // Increased number of concurrent upload workers
	uploadQueueCapacity = 200 // Increased capacity of the upload job queue
)

// uploadJob represents a single file chunk to be uploaded to Telegram.
type uploadJob struct {
	ctx        context.Context
	partName   string
	reader     io.Reader // Changed from io.ReaderAt
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
	ObjectLayer                   // Embeds ALL native MinIO methods automatically!
	base              ObjectLayer // Reference to native layer for delegation
	tgClient          *telegram.Client
	db                *sql.DB
	config            *TelegramConfig
	channelAccessHash int64
	hashMu            sync.RWMutex
	readyCh           chan struct{}
	uploadQueue       chan uploadJob
	runCtx            context.Context
}

// uploadWorker is a background worker that processes upload jobs from the queue.
func (t *TelegramObjectLayer) uploadWorker() {
	// Each worker has its own uploader and sender instance.
	// Using 8 threads per worker for faster chunk uploads.
	u := uploader.NewUploader(t.tgClient.API()).WithThreads(8).WithPartSize(512 * 1024)
	sender := message.NewSender(t.tgClient.API()).WithUploader(u)

	// The target channel for uploads is stable, so we can get it once.
	t.hashMu.RLock()
	target := sender.To(&tg.InputPeerChannel{
		ChannelID:  t.config.BareChannelID,
		AccessHash: t.channelAccessHash,
	})
	t.hashMu.RUnlock()

	for job := range t.uploadQueue {
		func() {
			var upload tg.InputFileClass

			// Ensure we close the reader after the job is done
			if closer, ok := job.reader.(io.Closer); ok {
				defer closer.Close()
			}

			// Phase 1: Upload the file data with retries
			buf := make([]byte, 512*1024)
			var b [8]byte
			rand.Read(b[:])
			fileID := int64(binary.LittleEndian.Uint64(b[:]))
			if fileID < 0 {
				fileID = -fileID
			}

			totalParts := int((job.size + int64(len(buf)) - 1) / int64(len(buf)))
			partIdx := 0
			var uploadErr error

			for {
				n, readErr := io.ReadFull(job.reader, buf)
				if readErr != nil && readErr != io.EOF && readErr != io.ErrUnexpectedEOF {
					uploadErr = fmt.Errorf("read failed: %w", readErr)
					break
				}
				if n == 0 {
					break
				}

				chunkData := buf[:n]
				var chunkErr error
				api := t.tgClient.API()

				for retries := 0; retries < 15; retries++ {
					t.hashMu.RLock()
					reqCtx := t.runCtx
					t.hashMu.RUnlock()
					if reqCtx == nil {
						reqCtx = job.ctx
					}

					_, chunkErr = api.UploadSaveBigFilePart(reqCtx, &tg.UploadSaveBigFilePartRequest{
						FileID:         fileID,
						FilePart:       partIdx,
						FileTotalParts: totalParts,
						Bytes:          chunkData,
					})

					if chunkErr == nil {
						break
					}

					if d, ok := tgerr.AsFloodWait(chunkErr); ok {
						time.Sleep(d + time.Second)
						continue
					}

					fmt.Printf("Worker chunk upload attempt %d failed for %s part %d: %v. Retrying...\n", retries+1, job.partName, partIdx, chunkErr)
					time.Sleep(time.Duration(retries+1) * 2 * time.Second)
				}

				if chunkErr != nil {
					uploadErr = fmt.Errorf("chunk %d upload failed after retries: %w", partIdx, chunkErr)
					break
				}
				partIdx++
			}

			if uploadErr != nil {
				job.resultChan <- uploadResult{err: uploadErr}
				return
			}

			upload = &tg.InputFileBig{
				ID:    fileID,
				Parts: totalParts,
				Name:  job.partName,
			}

			var msgUpdates tg.UpdatesClass
			var lastErr error

			t.hashMu.RLock()
			sendCtx := t.runCtx
			t.hashMu.RUnlock()
			if sendCtx == nil {
				sendCtx = job.ctx
			}

			// Phase 2: Send the file message with retries
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
					time.Sleep(time.Duration(retries+1) * 2 * time.Second)
					continue
				}
				break
			}

			if lastErr != nil {
				job.resultChan <- uploadResult{err: fmt.Errorf("worker send failed after retries: %w", lastErr)}
				return
			}

			// Extract Message ID from the response
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

	opts := telegram.Options{
		SessionStorage: &session.FileStorage{
			Path: "tg_session.json",
		},
	}
	if cfg.ProxyURL != "" {
		u, err := url.Parse(cfg.ProxyURL)
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
				return nil, fmt.Errorf("invalid proxy secret, must be a valid hex string (got '%s'): %w", secretHex, err)
			}

			addr := net.JoinHostPort(server, port)
			resolver, err := dcs.MTProxy(addr, secret, dcs.MTProxyOptions{})
			if err != nil {
				return nil, fmt.Errorf("failed to create MTProxy resolver: %w", err)
			}
			opts.Resolver = resolver
			fmt.Printf("Using MTProto proxy: %s\n", addr)
		}
	}

	client := telegram.NewClient(cfg.AppID, cfg.AppHash, opts)
	bgCtx := context.Background()

	layer := &TelegramObjectLayer{
		ObjectLayer: base, // Expose all native MinIO functions to the WebUI!
		base:        base,
		db:          db,
		config:      cfg,
		tgClient:    client,
		readyCh:     make(chan struct{}),
		uploadQueue: make(chan uploadJob, uploadQueueCapacity),
	}

	var readyOnce sync.Once

	go func() {
		for {
			err := client.Run(bgCtx, func(ctx context.Context) error {
				authStatus, err := client.Auth().Status(ctx)
				if err != nil {
					return fmt.Errorf("failed to check auth status: %w", err)
				}
				if !authStatus.Authorized {
					_, authErr := client.Auth().Bot(ctx, cfg.BotToken)
					if authErr != nil {
						fmt.Printf("Fatal: Telegram Bot Auth failed: %v\n", authErr)
						return authErr
					}
				}

				api := client.API()
				chResult, err := api.ChannelsGetChannels(ctx, []tg.InputChannelClass{
					&tg.InputChannel{ChannelID: cfg.BareChannelID, AccessHash: 0},
				})
				if err != nil {
					fmt.Printf("Error fetching channel info: %v\n", err)
				}

				var hash int64
				if chats, ok := chResult.(*tg.MessagesChats); ok && len(chats.Chats) > 0 {
					chat := chats.Chats[0]
					switch ch := chat.(type) {
					case *tg.Channel:
						hash = ch.AccessHash
					case *tg.ChannelForbidden:
						hash = ch.AccessHash
						fmt.Printf("Warning: Channel is forbidden, but extracted hash: %d\n", ch.AccessHash)
					default:
						fmt.Printf("Unexpected chat type returned: %T\n", ch)
					}
				} else {
					fmt.Printf("Failed to extract channel info from chResult: %v\n", chResult)
				}

				layer.hashMu.Lock()
				if hash != 0 {
					layer.channelAccessHash = hash
				}
				layer.runCtx = ctx
				layer.hashMu.Unlock()

				fmt.Printf("Telegram client connected (channelAccessHash=%d)\n", layer.channelAccessHash)

				readyOnce.Do(func() {
					close(layer.readyCh)
					for i := 0; i < numUploadWorkers; i++ {
						go layer.uploadWorker()
					}
				})

				<-ctx.Done()
				return ctx.Err()
			})
			fmt.Printf("TELEGRAM CLIENT RUN EXITED with error: %v. Reconnecting in 3s...\n", err)
			time.Sleep(3 * time.Second)
		}
	}()

	setObjectLayer(layer)
	return layer, nil
}
