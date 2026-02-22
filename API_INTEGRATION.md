# API Integration Guide: Connecting to MinIO-Telegram S3 Storage

This guide explains how to integrate your applications with the MinIO-Telegram S3-compatible object storage.

## Overview

The server provides a standard Amazon S3-compatible API. This means you can use any official AWS S3 SDK, MinIO SDK, or any S3-compatible tool to connect and manage your files. The underlying storage mechanism (Telegram) is completely transparent to the client application.

## Connection Credentials

To connect your application, you will need the following information, which you configure when you run the server:

-   **Endpoint URL**: The IP address and port where the MinIO server is running. The default port is `9000`.
-   **Access Key ID**: The value you set for `MINIO_ROOT_USER`.
-   **Secret Access Key**: The value you set for `MINIO_ROOT_PASSWORD`.
-   **Region**: For MinIO, this is not as strict as AWS. You can typically use `us-east-1` as a default.

**Example Configuration:**

| Parameter           | Example Value              | Environment Variable    |
| ------------------- | -------------------------- | ----------------------- |
| Endpoint URL        | `http://127.0.0.1:9000`      | (Server address)        |
| Access Key ID       | `minioadmin`               | `MINIO_ROOT_USER`       |
| Secret Access Key   | `minioadmin`               | `MINIO_ROOT_PASSWORD`   |
| Region              | `us-east-1`                | (Client configuration)  |

---

## Code Examples

Below are examples of how to connect and perform basic file operations in popular programming languages.

### Python (with `boto3`)

First, install the library: `pip install boto3`

```python
import boto3
from botocore.client import Config

# Configure the S3 client
s3 = boto3.client(
    's3',
    endpoint_url='http://127.0.0.1:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin',
    config=Config(signature_version='s3v4'),
    region_name='us-east-1'
)

bucket_name = 'my-bucket'
file_path = 'local_file.txt'
object_name = 'remote_file.txt'

# Upload a file
try:
    with open(file_path, "rb") as f:
        s3.upload_fileobj(f, bucket_name, object_name)
    print(f"Successfully uploaded {file_path} to {bucket_name}/{object_name}")
except Exception as e:
    print(f"Upload failed: {e}")

# Download a file
try:
    s3.download_file(bucket_name, object_name, 'downloaded_file.txt')
    print(f"Successfully downloaded {object_name} to downloaded_file.txt")
except Exception as e:
    print(f"Download failed: {e}")
```

### JavaScript/Node.js (with AWS SDK v3)

First, install the library: `npm install @aws-sdk/client-s3`

```javascript
import { S3Client, PutObjectCommand, GetObjectCommand } from "@aws-sdk/client-s3";
import { createWriteStream } from "fs";
import { Readable } from "stream";

// Configure the S3 client
const s3Client = new S3Client({
    endpoint: "http://127.0.0.1:9000",
    region: "us-east-1",
    credentials: {
        accessKeyId: "minioadmin",
        secretAccessKey: "minioadmin",
    },
    forcePathStyle: true, // Required for MinIO
});

const bucketName = "my-bucket";
const objectName = "remote_file.txt";

// Upload a file
async function uploadFile() {
    const putCommand = new PutObjectCommand({
        Bucket: bucketName,
        Key: objectName,
        Body: "Hello, S3 from JavaScript!",
    });

    try {
        const response = await s3Client.send(putCommand);
        console.log("Successfully uploaded file:", response);
    } catch (err) {
        console.error("Upload failed:", err);
    }
}

// Download a file
async function downloadFile() {
    const getCommand = new GetObjectCommand({
        Bucket: bucketName,
        Key: objectName,
    });

    try {
        const response = await s3Client.send(getCommand);
        const stream = response.Body;
        if (stream instanceof Readable) {
            const writer = createWriteStream("downloaded_file.js.txt");
            stream.pipe(writer);
            console.log("Successfully downloaded file.");
        }
    } catch (err) {
        console.error("Download failed:", err);
    }
}

uploadFile();
// downloadFile();
```

### Go (with `minio-go`)

First, install the library: `go get github.com/minio/minio-go/v7`

```go
package main

import (
	"context"
	"log"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func main() {
	ctx := context.Background()
	endpoint := "127.0.0.1:9000"
	accessKeyID := "minioadmin"
	secretAccessKey := "minioadmin"
	useSSL := false

	// Initialize minio client object.
	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		log.Fatalln(err)
	}

	bucketName := "my-bucket"
	objectName := "remote_file.txt"
	filePath := "./local_file.txt"
	contentType := "application/octet-stream"

	// Upload the file
	info, err := minioClient.FPutObject(ctx, bucketName, objectName, filePath, minio.PutObjectOptions{ContentType: contentType})
	if err != nil {
		log.Fatalln(err)
	}
	log.Printf("Successfully uploaded %s of size %d
", objectName, info.Size)

	// Download the file
	err = minioClient.FGetObject(ctx, bucketName, objectName, "downloaded_file.go.txt", minio.GetObjectOptions{})
	if err != nil {
		log.Fatalln(err)
	}
	log.Printf("Successfully downloaded %s", objectName)
}
```

---

## Using the MinIO Client (`mc`)

The MinIO Client (`mc`) is a powerful command-line tool for interacting with S3-compatible storage.

### 1. Configure an Alias

First, create an alias for your MinIO-Telegram server instance.

```bash
mc alias set my-telegram-s3 http://127.0.0.1:9000 minioadmin minioadmin
```

### 2. Run Commands

Now you can use standard UNIX-like commands to manage your data.

```bash
# Make a bucket
mc mb my-telegram-s3/my-first-bucket

# Upload a file
mc cp /path/to/your/file.txt my-telegram-s3/my-first-bucket/

# List contents
mc ls my-telegram-s3/my-first-bucket/

# Download a file
mc cp my-telegram-s3/my-first-bucket/remote_file.txt ./
```

---

## Important Considerations for the Telegram Backend

Because this MinIO instance uses Telegram as its core storage mechanism, there are a few backend-specific constraints S3 clients should be aware of:

1. **Upload Size Limits**: Depending on the Telegram API limits and backend chunking configuration, extremely large single-PUT uploads might be constrained. Multipart uploads are currently stubbed.
2. **Download Latency (Proxies)**: If the MinIO server is connected to Telegram via an MTProto Proxy, downloads might experience momentary stutters as the server actively reconstructs dropped chunks. S3 SDKs with default timeout settings might need extended timeout values.
3. **Session State**: The MinIO server must maintain a persistent `tg_session.json` file. If the storage administrator loses this session or migrates the server without it, the bot token can hit a `FLOOD_WAIT` rate limit from Telegram, causing temporary API failures for all clients.
4. **Access Isolation**: Files uploaded by a specific Telegram Bot token can only be downloaded by that exact same Bot token due to Telegram privacy policies.
