# MinIO Telegram Storage Gateway

This modification allows MinIO to use Telegram as an object storage backend, with PostgreSQL handling metadata.

## Prerequisites

- **Go 1.25+**
- **PostgreSQL** database accessible via connection URL.
- **Telegram App credentials** (`API_ID`, `API_HASH`) from [telegram.org](https://my.telegram.org).
- **Telegram Bot Token** (from @BotFather).
- **Target Channel ID** (where files will be stored). The bot must be an admin in this channel.

## Setup

1.  **Dependencies**:
    Run the following in the `minio` directory to fetch the modules:
    ```bash
    go mod tidy
    ```

2.  **Environment Variables**:
    Create a script or export these variables:

    ```bash
    export MINIO_TELEGRAM_ENABLED=on
    export TELEGRAM_API_ID=your_api_id
    export TELEGRAM_API_HASH=your_api_hash
    export TELEGRAM_BOT_TOKEN=your_bot_token
    export TELEGRAM_CHANNEL_ID=-100xxxxxxxxxx  # Ensure it starts with -100 for channels
    export POSTGRES_URL=postgresql://user:password@localhost:5432/dbname
    export TG_PROXY=tg://proxy?server=...  # Optional: MTProto proxy for reliable connections
    
    # MinIO Standard Envs (Optional)
    export MINIO_ROOT_USER=minioadmin
    export MINIO_ROOT_PASSWORD=minioadmin
    ```

3.  **Build**:
    ```bash
    cd minio/cmd # or just minio
    go build -o minio_tg .
    ```

4.  **Run**:
    ```bash
    ./minio_tg server /tmp/data
    ```
    *(Note: `/tmp/data` is just a placeholder, the storage backend will bypass disk and use Telegram)*

## Usage

Use `mc` (MinIO Client) to interact with the gateway:

```bash
mc alias set mytg http://localhost:9000 minioadmin minioadmin
mc mb mytg/testbucket
mc cp myphoto.jpg mytg/testbucket/
mc ls mytg/testbucket/
mc rm mytg/testbucket/myphoto.jpg
```

## Implementation Details

- **Metadata**: Stored in a PostgreSQL database table (`objects`).
- **Storage**: Files are uploaded to the specified Telegram channel in 20MB chunks (`parts` tracking).
- **Session Persistence**: Authentication utilizes a local `tg_session.json` to prevent re-authentication on every server restart. Without this, the bot token can encounter `FLOOD_WAIT (1556 seconds)` closures.
- **Resilient Downloads**: Manual `api.UploadGetFile` chunk fetching is employed with a connection recovery loop (~15 retries) to withstand TCP disconnects common to MTProto proxies.

## Limitations

- **Bot Access Isolation**: Telegram enforces strict privacy. Bots **cannot read files uploaded by other bots** even if they are both admins. Always use the identical bot token for uploads and downloads.
- **Range Requests**: Fully fetching arbitrary byte chunks is supported, but sequential downloading over unreliable proxy networks might incur retry latency.
- **Authentication**: Uses Bot API (via `gotd`). Ensure the bot has rights to upload to the channel.
