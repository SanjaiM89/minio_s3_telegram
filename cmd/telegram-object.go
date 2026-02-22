package cmd

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
)

// MaxTelegramChunkSize is the maximum size of a single Telegram file upload
const (
	// MaxTelegramChunkSize is the Telegram 2GB limit, we use 1.9GB to be safe
	MaxTelegramChunkSize = 1900 * 1024 * 1024
)

// ObjectMetadata holds metadata for an object stored in the Telegram backend
type ObjectMetadata struct {
	Bucket      string    `json:"bucket"`
	Key         string    `json:"key"`
	Size        int64     `json:"size"`
	ETag        string    `json:"etag"`
	ContentType string    `json:"content_type"`
	Parts       []int     `json:"parts"` // Message IDs
	ModTime     time.Time `json:"mod_time"`
}

func (t *TelegramObjectLayer) PutObject(ctx context.Context, bucket, object string, data *PutObjReader, opts ObjectOptions) (objInfo ObjectInfo, err error) {
	// Delegate system files to native MinIO!
	if t.isSystemBucket(bucket) {
		return t.base.PutObject(ctx, bucket, object, data, opts)
	}

	// Wait for Telegram client to be ready
	if err := t.tgReady(ctx); err != nil {
		return ObjectInfo{}, err
	}

	// For small files (< 1KB), we can buffer in memory and store in DB
	const smallFileLimit = 1024
	streamingChunkSize := int64(MaxTelegramChunkSize)

	var msgIDs []int
	var internalData []byte
	var totalSize int64

	// Cleanup function for temp files
	var tempFiles []string
	cleanup := func() {
		for _, f := range tempFiles {
			if f != "" {
				os.Remove(f)
			}
		}
	}
	defer cleanup()

	// Buffer to check small file limit
	headerBuffer := make([]byte, smallFileLimit)
	n, err := io.ReadFull(data.Reader, headerBuffer)

	// Handle errors, but allow EOF if file is smaller than smallFileLimit
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return ObjectInfo{}, fmt.Errorf("failed to read initial data: %w", err)
	}

	if n < smallFileLimit {
		// Small file case: < 1KB
		internalData = headerBuffer[:n]
		totalSize = int64(n)
	} else {
		// Large file case: >= 1KB
		totalSize = 0
		partNum := 1

		// Create first part from what we already read
		tmpFile, err := os.CreateTemp("", fmt.Sprintf("minio-tg-part-%d-*", partNum))
		if err != nil {
			return ObjectInfo{}, fmt.Errorf("failed to create temp file for part %d: %w", partNum, err)
		}
		tempFiles = append(tempFiles, tmpFile.Name())

		wn, _ := tmpFile.Write(headerBuffer[:n])
		totalSize += int64(wn)

		var resultChans []chan uploadResult

		// Continue reading and uploading chunks
		eof := false
		for !eof {
			// Read the rest of the chunk
			m, err := io.CopyN(tmpFile, data.Reader, streamingChunkSize-int64(wn))
			if err != nil {
				if err == io.EOF {
					eof = true
				} else {
					tmpFile.Close()
					return ObjectInfo{}, fmt.Errorf("failed to read from client: %w", err)
				}
			}
			totalSize += m
			chunkActualSize := int64(wn) + m
			tmpFile.Close()

			// Queue the chunk for upload
			resChan := make(chan uploadResult, 1)
			reopenedFile, _ := os.Open(tmpFile.Name())

			job := uploadJob{
				ctx:        ctx,
				partName:   fmt.Sprintf("%s.part%d", object, partNum),
				reader:     reopenedFile,
				size:       chunkActualSize,
				resultChan: resChan,
			}

			select {
			case t.uploadQueue <- job:
				resultChans = append(resultChans, resChan)
			case <-ctx.Done():
				reopenedFile.Close()
				return ObjectInfo{}, ctx.Err()
			}

			if !eof {
				partNum++
				wn = 0 // Reset for next chunk
				tmpFile, err = os.CreateTemp("", fmt.Sprintf("minio-tg-part-%d-*", partNum))
				if err != nil {
					return ObjectInfo{}, fmt.Errorf("failed to create temp file for part %d: %w", partNum, err)
				}
				tempFiles = append(tempFiles, tmpFile.Name())
			}
		}

		// Wait for all chunk uploads to complete
		for i, resChan := range resultChans {
			select {
			case result := <-resChan:
				if result.err != nil {
					return ObjectInfo{}, fmt.Errorf("upload worker failed for part %d: %w", i+1, result.err)
				}
				msgIDs = append(msgIDs, result.msgID)
				os.Remove(tempFiles[i])
				tempFiles[i] = ""
			case <-ctx.Done():
				return ObjectInfo{}, ctx.Err()
			}
		}
	}

	meta := ObjectMetadata{
		Bucket:      bucket,
		Key:         object,
		Size:        totalSize,
		ETag:        data.MD5CurrentHexString(),
		ContentType: opts.UserDefined["content-type"],
		Parts:       msgIDs,
		ModTime:     time.Now().UTC(),
	}
	if meta.ContentType == "" {
		meta.ContentType = "application/octet-stream"
	}

	metaBytes, _ := json.Marshal(meta)

	query := `
		INSERT INTO objects (bucket, key, metadata, data)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (bucket, key)
		DO UPDATE SET metadata = EXCLUDED.metadata, data = EXCLUDED.data
	`
	if _, err := t.db.ExecContext(ctx, query, bucket, object, metaBytes, internalData); err != nil {
		return ObjectInfo{}, fmt.Errorf("postgres upsert failed: %w", err)
	}

	return ObjectInfo{
		Bucket:      bucket,
		Name:        object,
		Size:        totalSize,
		ModTime:     meta.ModTime,
		ETag:        meta.ETag,
		ContentType: meta.ContentType,
	}, nil
}

// trackingWriter tracks total bytes written to a writer
type trackingWriter struct {
	io.Writer
	total *int64
}

func (tw *trackingWriter) Write(p []byte) (n int, err error) {
	n, err = tw.Writer.Write(p)
	if n > 0 {
		*tw.total += int64(n)
	}
	if err != nil {
		fmt.Printf("trackingWriter Write Error: %v\n", err)
	}
	return n, err
}

func (t *TelegramObjectLayer) GetObjectNInfo(ctx context.Context, bucket, object string, rs *HTTPRangeSpec, h http.Header, opts ObjectOptions) (gr *GetObjectReader, err error) {
	// Delegate system files to native MinIO!
	if t.isSystemBucket(bucket) {
		return t.base.GetObjectNInfo(ctx, bucket, object, rs, h, opts)
	}

	// Wait for Telegram client to be ready
	if err := t.tgReady(ctx); err != nil {
		return nil, err
	}

	var metaBytes []byte
	var internalData []byte
	err = t.db.QueryRowContext(ctx, "SELECT metadata, data FROM objects WHERE bucket=$1 AND key=$2", bucket, object).Scan(&metaBytes, &internalData)
	if err == sql.ErrNoRows {
		return nil, ObjectNotFound{Bucket: bucket, Object: object}
	} else if err != nil {
		return nil, err
	}

	var meta ObjectMetadata
	if err := json.Unmarshal(metaBytes, &meta); err != nil {
		return nil, err
	}

	objInfo := ObjectInfo{
		Bucket:      bucket,
		Name:        object,
		Size:        meta.Size,
		ModTime:     meta.ModTime,
		ETag:        meta.ETag,
		ContentType: meta.ContentType,
	}

	if len(internalData) > 0 {
		return NewGetObjectReaderFromReader(bytes.NewReader(internalData), objInfo, opts)
	}

	pr, pw := io.Pipe()

	go func() {
		streamCtx := t.runCtx
		var totalBytesWritten int64
		// Create a writer that logs EVERY byte written and every error encountered to debug pipe closures.
		tw := &trackingWriter{Writer: pw, total: &totalBytesWritten}

		defer func() {
			pw.Close()
			fmt.Printf("Telegram Download Goroutine Exited. Total Written: %d, Expected: %d\n", totalBytesWritten, objInfo.Size)
		}()

		api := t.tgClient.API()

		type partResult struct {
			path string
			err  error
		}

		numParts := len(meta.Parts)
		results := make([]chan partResult, numParts)
		for i := range results {
			results[i] = make(chan partResult, 1)
		}

		// Parallel pre-fetching for subsequent parts
		const maxParallelDownloads = 3
		sem := make(chan struct{}, maxParallelDownloads)

		for i, msgID := range meta.Parts {
			if i == 0 {
				continue // First part is streamed directly
			}

			go func(idx int, mid int) {
				sem <- struct{}{}
				defer func() { <-sem }()

				var resp tg.MessagesMessagesClass
				var getErr error
				for retries := 0; retries < 15; retries++ {
					t.hashMu.RLock()
					resp, getErr = api.ChannelsGetMessages(streamCtx, &tg.ChannelsGetMessagesRequest{
						Channel: &tg.InputChannel{ChannelID: t.config.BareChannelID, AccessHash: t.channelAccessHash},
						ID:      []tg.InputMessageClass{&tg.InputMessageID{ID: mid}},
					})
					t.hashMu.RUnlock()
					if getErr == nil {
						break
					}
					if d, ok := tgerr.AsFloodWait(getErr); ok {
						time.Sleep(d + time.Second)
						continue
					}
					time.Sleep(time.Duration(retries+1) * 2 * time.Second)
				}
				if getErr != nil {
					results[idx] <- partResult{err: fmt.Errorf("pre-fetch failed: %w", getErr)}
					return
				}

				msg, _ := t.extractMessage(resp)
				doc, _ := t.extractDocument(msg)
				if doc == nil {
					results[idx] <- partResult{err: fmt.Errorf("invalid document in part %d", idx)}
					return
				}

				tmpFile, err := os.CreateTemp("", fmt.Sprintf("minio-tg-pre-%d-*", mid))
				if err != nil {
					results[idx] <- partResult{err: err}
					return
				}
				defer tmpFile.Close()

				loc := doc.AsInputDocumentFileLocation()
				offset := int64(0)
				limit := 512 * 1024

				for offset < doc.Size {
					var chunkData []byte
					var dlErr error

					for retries := 0; retries < 15; retries++ {
						t.hashMu.RLock()
						req := &tg.UploadGetFileRequest{
							Offset:   offset,
							Limit:    limit,
							Location: loc,
						}
						res, err := api.UploadGetFile(streamCtx, req)
						t.hashMu.RUnlock()

						if err == nil {
							switch f := res.(type) {
							case *tg.UploadFile:
								chunkData = f.Bytes
							case *tg.UploadFileCDNRedirect:
								dlErr = fmt.Errorf("CDN redirect not supported")
							default:
								dlErr = fmt.Errorf("unexpected file type: %T", res)
							}
							if dlErr == nil {
								break
							}
						} else {
							dlErr = err
						}

						if d, ok := tgerr.AsFloodWait(dlErr); ok {
							time.Sleep(d + time.Second)
							continue
						}
						time.Sleep(time.Duration(retries+1) * 2 * time.Second)
					}

					if dlErr != nil {
						err = dlErr
						break
					}
					if len(chunkData) == 0 {
						break
					}
					_, err = tmpFile.Write(chunkData)
					if err != nil {
						break
					}
					offset += int64(len(chunkData))
				}

				if err != nil {
					os.Remove(tmpFile.Name())
					results[idx] <- partResult{err: err}
					return
				}

				results[idx] <- partResult{path: tmpFile.Name()}
			}(i, msgID)
		}

		// Consumer loop
		for i, msgID := range meta.Parts {
			if i == 0 {
				// Part 0: Stream directly
				var resp tg.MessagesMessagesClass
				var getErr error
				for retries := 0; retries < 15; retries++ {
					t.hashMu.RLock()
					resp, getErr = api.ChannelsGetMessages(streamCtx, &tg.ChannelsGetMessagesRequest{
						Channel: &tg.InputChannel{ChannelID: t.config.BareChannelID, AccessHash: t.channelAccessHash},
						ID:      []tg.InputMessageClass{&tg.InputMessageID{ID: msgID}},
					})
					t.hashMu.RUnlock()
					if getErr == nil {
						break
					}
					if d, ok := tgerr.AsFloodWait(getErr); ok {
						time.Sleep(d + time.Second)
						continue
					}
					time.Sleep(time.Duration(retries+1) * 2 * time.Second)
				}
				if getErr != nil {
					fmt.Printf("Telegram Error fetching msg %d: %v\n", msgID, getErr)
					pw.CloseWithError(getErr)
					return
				}

				msg, okMsg := t.extractMessage(resp)
				doc, okDoc := t.extractDocument(msg)
				if doc == nil {
					fmt.Printf("Telegram extraction failed for msg %d. msg_ok:%v doc_ok:%v\n", msgID, okMsg, okDoc)
					pw.CloseWithError(fmt.Errorf("first part document invalid"))
					return
				}

				fmt.Printf("Telegram Starting stream for %s/%s size %d from msg %d (using runCtx)\n", bucket, object, doc.Size, msgID)

				loc := doc.AsInputDocumentFileLocation()
				offset := int64(0)
				limit := 512 * 1024

				for offset < doc.Size {
					var chunkData []byte
					var dlErr error

					for retries := 0; retries < 15; retries++ {
						t.hashMu.RLock()
						req := &tg.UploadGetFileRequest{
							Offset:   offset,
							Limit:    limit,
							Location: loc,
						}
						res, err := api.UploadGetFile(streamCtx, req)
						t.hashMu.RUnlock()

						if err == nil {
							switch f := res.(type) {
							case *tg.UploadFile:
								chunkData = f.Bytes
							case *tg.UploadFileCDNRedirect:
								dlErr = fmt.Errorf("CDN redirect not supported")
							default:
								dlErr = fmt.Errorf("unexpected file type: %T", res)
							}
							if dlErr == nil {
								break
							}
						} else {
							dlErr = err
						}

						if d, ok := tgerr.AsFloodWait(dlErr); ok {
							time.Sleep(d + time.Second)
							continue
						}
						fmt.Printf("Telegram Chunk fetch retry %d at offset %d: %v\n", retries+1, offset, dlErr)
						time.Sleep(time.Duration(retries+1) * 2 * time.Second)
					}

					if dlErr != nil {
						fmt.Printf("Telegram Error downloading chunk at offset %d: %v\n", offset, dlErr)
						pw.CloseWithError(dlErr)
						return
					}

					if len(chunkData) == 0 {
						break // EOF reached
					}

					_, writeErr := tw.Write(chunkData)
					if writeErr != nil {
						pw.CloseWithError(writeErr)
						return
					}

					offset += int64(len(chunkData))
				}
				continue
			}

			// Subsequent parts: from Disk
			select {
			case res := <-results[i]:
				if res.err != nil {
					pw.CloseWithError(res.err)
					return
				}
				f, err := os.Open(res.path)
				if err != nil {
					pw.CloseWithError(err)
					return
				}
				_, err = io.Copy(tw, f)
				f.Close()
				os.Remove(res.path)
				if err != nil {
					if !errors.Is(err, io.ErrClosedPipe) {
						pw.CloseWithError(err)
					}
					return
				}
			case <-streamCtx.Done():
				return
			}
		}
	}()

	// Use the actual object size from metadata to satisfy MinIO's Content-Length
	// and HTTPRangeSpec validation checks.
	return NewGetObjectReaderFromReader(pr, objInfo, opts)
}

// Helper methods to reduce duplication
func (t *TelegramObjectLayer) extractMessage(resp tg.MessagesMessagesClass) (*tg.Message, bool) {
	if resp == nil {
		fmt.Printf("extractMessage: resp is nil\n")
		return nil, false
	}
	fmt.Printf("extractMessage: resp type is %T\n", resp)

	var msg *tg.Message
	switch msgs := resp.(type) {
	case *tg.MessagesMessages:
		fmt.Printf("extractMessage: MessagesMessages length %d\n", len(msgs.Messages))
		if len(msgs.Messages) > 0 {
			fmt.Printf("extractMessage: first message type is %T\n", msgs.Messages[0])
			msg, _ = msgs.Messages[0].(*tg.Message)
		}
	case *tg.MessagesMessagesSlice:
		fmt.Printf("extractMessage: MessagesMessagesSlice length %d\n", len(msgs.Messages))
		if len(msgs.Messages) > 0 {
			fmt.Printf("extractMessage: first message type is %T\n", msgs.Messages[0])
			msg, _ = msgs.Messages[0].(*tg.Message)
		}
	case *tg.MessagesChannelMessages:
		fmt.Printf("extractMessage: MessagesChannelMessages length %d\n", len(msgs.Messages))
		if len(msgs.Messages) > 0 {
			fmt.Printf("extractMessage: first message type is %T\n", msgs.Messages[0])
			msg, _ = msgs.Messages[0].(*tg.Message)
		}
	default:
		fmt.Printf("extractMessage: unknown MessagesMessagesClass type: %T\n", msgs)
	}
	return msg, msg != nil
}

func (t *TelegramObjectLayer) extractDocument(msg *tg.Message) (*tg.Document, bool) {
	if msg == nil {
		return nil, false
	}
	media, ok := msg.Media.(*tg.MessageMediaDocument)
	if !ok {
		return nil, false
	}
	doc, ok := media.Document.(*tg.Document)
	return doc, ok
}

func (t *TelegramObjectLayer) DeleteObject(ctx context.Context, bucket, object string, opts ObjectOptions) (ObjectInfo, error) {
	if t.isSystemBucket(bucket) {
		return t.base.DeleteObject(ctx, bucket, object, opts)
	}

	var metaBytes []byte
	err := t.db.QueryRowContext(ctx, "SELECT metadata FROM objects WHERE bucket=$1 AND key=$2", bucket, object).Scan(&metaBytes)
	if err == sql.ErrNoRows {
		// MinIO expects an empty ObjectInfo on successful delete of a non-existent object
		return ObjectInfo{}, nil
	}

	var meta ObjectMetadata
	json.Unmarshal(metaBytes, &meta)

	t.db.ExecContext(ctx, "DELETE FROM objects WHERE bucket=$1 AND key=$2", bucket, object)

	if len(meta.Parts) > 0 {
		go func() {
			t.hashMu.RLock()
			// Best effort deletion
			t.tgClient.API().ChannelsDeleteMessages(context.Background(), &tg.ChannelsDeleteMessagesRequest{
				Channel: &tg.InputChannel{ChannelID: t.config.BareChannelID, AccessHash: t.channelAccessHash},
				ID:      meta.Parts,
			})
			t.hashMu.RUnlock()
		}()
	}

	return ObjectInfo{
		Bucket: bucket,
		Name:   object,
	}, nil
}

func (t *TelegramObjectLayer) GetObjectInfo(ctx context.Context, bucket, object string, opts ObjectOptions) (objInfo ObjectInfo, err error) {
	if t.isSystemBucket(bucket) {
		return t.base.GetObjectInfo(ctx, bucket, object, opts)
	}

	var metaBytes []byte
	err = t.db.QueryRowContext(ctx, "SELECT metadata FROM objects WHERE bucket=$1 AND key=$2", bucket, object).Scan(&metaBytes)
	if err == sql.ErrNoRows {
		return ObjectInfo{}, ObjectNotFound{Bucket: bucket, Object: object}
	} else if err != nil {
		return ObjectInfo{}, err
	}

	var meta ObjectMetadata
	if err := json.Unmarshal(metaBytes, &meta); err != nil {
		return ObjectInfo{}, err
	}

	return ObjectInfo{
		Bucket:      bucket,
		Name:        object,
		Size:        meta.Size,
		ModTime:     meta.ModTime,
		ETag:        meta.ETag,
		ContentType: meta.ContentType,
	}, nil
}

func (t *TelegramObjectLayer) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (ListObjectsInfo, error) {
	query := "SELECT metadata FROM objects WHERE bucket=$1 AND key LIKE $2 AND key > $3 ORDER BY key ASC LIMIT $4"
	rows, err := t.db.QueryContext(ctx, query, bucket, prefix+"%", marker, maxKeys)
	if err != nil {
		return ListObjectsInfo{}, err
	}
	defer rows.Close()

	var objects []ObjectInfo
	var lastKey string
	for rows.Next() {
		var metaBytes []byte
		if err := rows.Scan(&metaBytes); err != nil {
			continue
		}
		var meta ObjectMetadata
		if err := json.Unmarshal(metaBytes, &meta); err == nil {
			objects = append(objects, ObjectInfo{
				Bucket:  meta.Bucket,
				Name:    meta.Key,
				Size:    meta.Size,
				ModTime: meta.ModTime,
				ETag:    meta.ETag,
			})
			lastKey = meta.Key
		}
	}

	isTruncated := len(objects) == maxKeys
	var nextMarker string
	if isTruncated {
		nextMarker = lastKey
	}

	return ListObjectsInfo{
		Objects:     objects,
		IsTruncated: isTruncated,
		NextMarker:  nextMarker,
	}, nil
}

func (t *TelegramObjectLayer) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (ListObjectsV2Info, error) {
	if t.isSystemBucket(bucket) {
		return t.base.ListObjectsV2(ctx, bucket, prefix, continuationToken, delimiter, maxKeys, fetchOwner, startAfter)
	}

	// Use the V1 list implementation
	info, err := t.ListObjects(ctx, bucket, prefix, continuationToken, delimiter, maxKeys)
	if err != nil {
		return ListObjectsV2Info{}, err
	}

	return ListObjectsV2Info{
		Objects:               info.Objects,
		IsTruncated:           info.IsTruncated,
		NextContinuationToken: info.NextMarker,
	}, nil
}

// These functions are not needed for the telegram backend, but are required by the ObjectLayer interface.
// The embedded base layer will handle them for system buckets.
// For user buckets, we can return NotImplemented or a sensible default.

func (t *TelegramObjectLayer) CopyObject(ctx context.Context, srcBucket, srcObject, destBucket, destObject string, srcInfo ObjectInfo, srcOpts, dstOpts ObjectOptions) (ObjectInfo, error) {
	if t.isSystemBucket(srcBucket) && t.isSystemBucket(destBucket) {
		return t.base.CopyObject(ctx, srcBucket, srcObject, destBucket, destObject, srcInfo, srcOpts, dstOpts)
	}
	return ObjectInfo{}, NotImplemented{}
}

func (t *TelegramObjectLayer) DeleteObjects(ctx context.Context, bucket string, objects []ObjectToDelete, opts ObjectOptions) ([]DeletedObject, []error) {
	if t.isSystemBucket(bucket) {
		return t.base.DeleteObjects(ctx, bucket, objects, opts)
	}
	errs := make([]error, len(objects))
	dobjects := make([]DeletedObject, len(objects))
	for i, obj := range objects {
		_, errs[i] = t.DeleteObject(ctx, bucket, obj.ObjectName, opts)
		if errs[i] == nil {
			dobjects[i] = DeletedObject{ObjectName: obj.ObjectName}
		}
	}
	return dobjects, errs
}

func (t *TelegramObjectLayer) ListObjectVersions(ctx context.Context, bucket, prefix, marker, versionMarker, delimiter string, maxKeys int) (ListObjectVersionsInfo, error) {
	if t.isSystemBucket(bucket) {
		return t.base.ListObjectVersions(ctx, bucket, prefix, marker, versionMarker, delimiter, maxKeys)
	}
	return ListObjectVersionsInfo{}, nil // No versioning for telegram backend
}
