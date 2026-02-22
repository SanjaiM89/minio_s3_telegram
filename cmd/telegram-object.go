package cmd

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
)

// MaxTelegramChunkSize is the Telegram 2GB limit, we use 1.9GB to be safe
const (
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
	if t.isSystemBucket(bucket) {
		return t.base.PutObject(ctx, bucket, object, data, opts)
	}

	if err := t.tgReady(ctx); err != nil {
		return ObjectInfo{}, err
	}

	const smallFileLimit = 1024
	streamingChunkSize := int64(MaxTelegramChunkSize)

	var msgIDs []int
	var internalData []byte
	var totalSize int64

	var tempFiles []string
	cleanup := func() {
		for _, f := range tempFiles {
			if f != "" {
				os.Remove(f)
			}
		}
	}
	defer cleanup()

	headerBuffer := make([]byte, smallFileLimit)
	n, err := io.ReadFull(data.Reader, headerBuffer)

	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return ObjectInfo{}, fmt.Errorf("failed to read initial data: %w", err)
	}

	if n < smallFileLimit {
		internalData = headerBuffer[:n]
		totalSize = int64(n)
	} else {
		totalSize = 0
		partNum := 1

		tmpFile, err := os.CreateTemp("", fmt.Sprintf("minio-tg-part-%d-*", partNum))
		if err != nil {
			return ObjectInfo{}, fmt.Errorf("failed to create temp file for part %d: %w", partNum, err)
		}
		tempFiles = append(tempFiles, tmpFile.Name())

		wn, _ := tmpFile.Write(headerBuffer[:n])
		totalSize += int64(wn)

		var resultChans []chan uploadResult

		eof := false
		for !eof {
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
				wn = 0
				tmpFile, err = os.CreateTemp("", fmt.Sprintf("minio-tg-part-%d-*", partNum))
				if err != nil {
					return ObjectInfo{}, fmt.Errorf("failed to create temp file for part %d: %w", partNum, err)
				}
				tempFiles = append(tempFiles, tmpFile.Name())
			}
		}

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
	return n, err
}

// batchFetchDocuments fetches all documents for the given message IDs in a single API call.
func (t *TelegramObjectLayer) batchFetchDocuments(ctx context.Context, api *tg.Client, msgIDs []int) (map[int]*tg.Document, error) {
	if len(msgIDs) == 0 {
		return nil, nil
	}

	ids := make([]tg.InputMessageClass, len(msgIDs))
	for i, msgID := range msgIDs {
		ids[i] = &tg.InputMessageID{ID: msgID}
	}

	hash := t.channelAccessHash.Load()
	fmt.Printf("DEBUG: batchFetchDocuments using ChannelID=%d, AccessHash=%d, numMsgIDs=%d\n", t.config.BareChannelID, hash, len(msgIDs))

	var resp tg.MessagesMessagesClass
	var lastErr error
	channelReq := func(accessHash int64) *tg.ChannelsGetMessagesRequest {
		return &tg.ChannelsGetMessagesRequest{
			Channel: &tg.InputChannel{ChannelID: t.config.BareChannelID, AccessHash: accessHash},
			ID:      ids,
		}
	}
	for retries := 0; retries < 15; retries++ {
		resp, lastErr = api.ChannelsGetMessages(ctx, channelReq(hash))
		if lastErr == nil {
			break
		}

		if tgerr.Is(lastErr, "CHANNEL_INVALID") || tgerr.Is(lastErr, "CHANNEL_PRIVATE") {
			if refreshedHash, refreshErr := t.resolveChannelAccessHash(ctx, api); refreshErr == nil && refreshedHash != 0 && refreshedHash != hash {
				hash = refreshedHash
				continue
			}
		}

		if d, ok := tgerr.AsFloodWait(lastErr); ok {
			time.Sleep(d + time.Second)
			continue
		}
		fmt.Printf("DEBUG: batchFetchDocuments attempt %d failed: %v\n", retries+1, lastErr)
		time.Sleep(time.Duration(retries+1) * 200 * time.Millisecond)
	}
	if lastErr != nil {
		fmt.Printf("DEBUG: batchFetchDocuments final error: %v\n", lastErr)
		return nil, lastErr
	}

	var rawMessages []tg.MessageClass
	switch m := resp.(type) {
	case *tg.MessagesMessages:
		rawMessages = m.Messages
	case *tg.MessagesMessagesSlice:
		rawMessages = m.Messages
	case *tg.MessagesChannelMessages:
		rawMessages = m.Messages
	}

	docs := make(map[int]*tg.Document, len(rawMessages))
	for _, raw := range rawMessages {
		msg, ok := raw.(*tg.Message)
		if !ok {
			continue
		}
		if doc, ok := t.extractDocument(msg); ok {
			docs[msg.ID] = doc
		}
	}
	return docs, nil
}

func (t *TelegramObjectLayer) downloadTelegramDocument(ctx context.Context, api *tg.Client, doc *tg.Document, w io.Writer, startOffset, length int64) error {
	if length == -1 {
		length = doc.Size - startOffset
	}

	const chunkSize = 1024 * 1024 // 1 MiB chunks (Telegram max)
	const maxParallelChunks = 32

	type chunkResult struct {
		offset int64
		data   []byte
		err    error
	}

	resultCh := make(chan chunkResult, maxParallelChunks*2)
	fetchCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	sem := make(chan struct{}, maxParallelChunks)

	go func() {
		// Telegram upload.getFile requires:
		// 1. limit is a multiple of 1024
		// 2. offset is a multiple of limit
		// 3. limit <= 1048576 (1 MiB)
		// We align our requests to chunkSize boundaries and trim later.

		requestedEnd := startOffset + length

		// First block might be unaligned
		firstBlockStart := (startOffset / int64(chunkSize)) * int64(chunkSize)

		for fetchOffset := firstBlockStart; fetchOffset < requestedEnd; fetchOffset += int64(chunkSize) {
			select {
			case <-fetchCtx.Done():
				return
			case sem <- struct{}{}:
			}

			go func(off int64) {
				var chunkData []byte
				var dlErr error
				downloadAPI := api
				currentDoc := doc
				currentLoc := currentDoc.AsInputDocumentFileLocation()

				for retries := 0; retries < 15; retries++ {
					// We always request a full chunkSize to satisfy alignment
					req := &tg.UploadGetFileRequest{
						Offset:   off,
						Limit:    chunkSize,
						Location: currentLoc,
					}
					res, err := downloadAPI.UploadGetFile(fetchCtx, req)
					if err == nil {
						switch f := res.(type) {
						case *tg.UploadFile:
							chunkData = f.Bytes
							dlErr = nil
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

					if tgerr.Is(dlErr, "FILE_REFERENCE_EXPIRED") {
						freshDoc, refreshErr := t.fetchSingleDocument(fetchCtx, currentDoc.ID)
						if refreshErr != nil {
							dlErr = fmt.Errorf("refresh file reference for doc %d failed: %w", currentDoc.ID, refreshErr)
							break
						}
						if freshDoc != nil {
							currentDoc = freshDoc
							currentLoc = freshDoc.AsInputDocumentFileLocation()
							downloadAPI = t.nextDownloadAPI()
							continue
						}
					}

					if d, ok := tgerr.AsFloodWait(dlErr); ok {
						time.Sleep(d + time.Second)
						continue
					}
					time.Sleep(time.Duration(retries+1) * 200 * time.Millisecond)
				}

				<-sem

				// Trim the chunk data if it exceeds actual document size
				if int64(len(chunkData)) > currentDoc.Size-off {
					chunkData = chunkData[:currentDoc.Size-off]
				}

				select {
				case resultCh <- chunkResult{offset: off, data: chunkData, err: dlErr}:
				case <-fetchCtx.Done():
				}
			}(fetchOffset)
		}
	}()

	// Sequencer: write chunks to w in order, trimming to the requested range
	currentBlockOffset := (startOffset / int64(chunkSize)) * int64(chunkSize)
	requestedEnd := startOffset + length
	received := make(map[int64][]byte)

	for currentBlockOffset < requestedEnd {
		if data, ok := received[currentBlockOffset]; ok {
			// Calculate overlap with requested range [startOffset, requestedEnd)
			blockEnd := currentBlockOffset + int64(len(data))

			writeStart := int64(0)
			if currentBlockOffset < startOffset {
				writeStart = startOffset - currentBlockOffset
			}

			writeEnd := int64(len(data))
			if blockEnd > requestedEnd {
				writeEnd = int64(len(data)) - (blockEnd - requestedEnd)
			}

			if writeStart < writeEnd {
				if _, err := w.Write(data[writeStart:writeEnd]); err != nil {
					return err
				}
			}

			delete(received, currentBlockOffset)
			currentBlockOffset += int64(chunkSize) // Move to next alignment boundary
			continue
		}

		select {
		case res := <-resultCh:
			if res.err != nil {
				return res.err
			}
			received[res.offset] = res.data
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

func (t *TelegramObjectLayer) fetchSingleDocument(ctx context.Context, msgID int) (*tg.Document, error) {
	if msgID == 0 {
		return nil, fmt.Errorf("invalid message id")
	}

	api := t.tgClient.API()
	docs, err := t.batchFetchDocuments(ctx, api, []int{msgID})
	if err != nil {
		return nil, err
	}
	doc := docs[msgID]
	if doc == nil {
		return nil, fmt.Errorf("document not found for message %d", msgID)
	}
	return doc, nil
}

func (t *TelegramObjectLayer) GetObjectNInfo(ctx context.Context, bucket, object string, rs *HTTPRangeSpec, h http.Header, opts ObjectOptions) (gr *GetObjectReader, err error) {
	if t.isSystemBucket(bucket) {
		return t.base.GetObjectNInfo(ctx, bucket, object, rs, h, opts)
	}

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

	startOffset, length, err := rs.GetOffsetLength(objInfo.Size)
	if err != nil {
		return nil, err
	}

	if len(internalData) > 0 {
		return NewGetObjectReaderFromReader(bytes.NewReader(internalData[startOffset:startOffset+length]), objInfo, opts)
	}

	pr, pw := io.Pipe()

	go func() {
		streamCtx := t.runCtx
		var totalBytesWritten int64

		// 16MB pipe buffer to prevent download goroutines from stalling
		bw := bufio.NewWriterSize(pw, 16*1024*1024)
		tw := &trackingWriter{Writer: bw, total: &totalBytesWritten}

		defer func() {
			bw.Flush()
			pw.Close()
		}()

		// --- KEY OPTIMIZATION: Batch-fetch ALL part documents in a single API call ---
		// We use the primary client for metadata as it's already "warmed up" with the channel access hash
		primaryAPI := t.tgClient.API()
		allDocs, err := t.batchFetchDocuments(streamCtx, primaryAPI, meta.Parts)
		if err != nil {
			pw.CloseWithError(fmt.Errorf("batch fetch documents failed: %w", err))
			return
		}

		const partSize = MaxTelegramChunkSize
		currentGlobalOffset := int64(0)

		for _, msgID := range meta.Parts {
			doc, ok := allDocs[msgID]
			if !ok {
				// Document missing from batch response; skip with fallback offset
				fmt.Printf("warning: document for msgID %d not found in batch response\n", msgID)
				currentGlobalOffset += partSize
				continue
			}

			partStart := currentGlobalOffset
			partEnd := currentGlobalOffset + doc.Size

			// Stop early if we've already served the entire requested range
			if currentGlobalOffset >= startOffset+length {
				break
			}

			// Check if this part overlaps with the requested range
			if startOffset < partEnd && (startOffset+length) > partStart {
				relativeStart := int64(0)
				if startOffset > partStart {
					relativeStart = startOffset - partStart
				}

				overlapEnd := partEnd
				if startOffset+length < partEnd {
					overlapEnd = startOffset + length
				}
				relativeLength := overlapEnd - (partStart + relativeStart)

				// Clamp to actual document size
				if relativeStart >= doc.Size {
					currentGlobalOffset += doc.Size
					continue
				}
				if relativeStart+relativeLength > doc.Size {
					relativeLength = doc.Size - relativeStart
				}

				// Round-robin across download clients for maximum throughput
				api := t.nextDownloadAPI()
				if err := t.downloadTelegramDocument(streamCtx, api, doc, tw, relativeStart, relativeLength); err != nil {
					pw.CloseWithError(err)
					return
				}
			}

			currentGlobalOffset += doc.Size
		}
	}()

	return NewGetObjectReaderFromReader(pr, objInfo, opts)
}

// Helper methods
func (t *TelegramObjectLayer) extractMessage(resp tg.MessagesMessagesClass) (*tg.Message, bool) {
	if resp == nil {
		return nil, false
	}
	var msg *tg.Message
	switch msgs := resp.(type) {
	case *tg.MessagesMessages:
		if len(msgs.Messages) > 0 {
			msg, _ = msgs.Messages[0].(*tg.Message)
		}
	case *tg.MessagesMessagesSlice:
		if len(msgs.Messages) > 0 {
			msg, _ = msgs.Messages[0].(*tg.Message)
		}
	case *tg.MessagesChannelMessages:
		if len(msgs.Messages) > 0 {
			msg, _ = msgs.Messages[0].(*tg.Message)
		}
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
		return ObjectInfo{}, nil
	}

	var meta ObjectMetadata
	json.Unmarshal(metaBytes, &meta)

	t.db.ExecContext(ctx, "DELETE FROM objects WHERE bucket=$1 AND key=$2", bucket, object)

	if len(meta.Parts) > 0 {
		go func() {
			hash := t.channelAccessHash.Load()
			t.tgClient.API().ChannelsDeleteMessages(context.Background(), &tg.ChannelsDeleteMessagesRequest{
				Channel: &tg.InputChannel{ChannelID: t.config.BareChannelID, AccessHash: hash},
				ID:      meta.Parts,
			})
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
	return ListObjectVersionsInfo{}, nil
}
