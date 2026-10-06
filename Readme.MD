
# Real-Time Crypto Analytics with PySpark Structured Streaming

Connects to the live **Coinbase Exchange WebSocket feed**, ingests it with **PySpark Structured Streaming**, and computes real-time windowed analytics (OHLC, volume, VWAP, spread, return %) per trading pair.

## Goals

- Connect to a data streaming feed with PySpark
- Analyse and transform the streaming data with PySpark

## Architecture

```
Coinbase WebSocket ──► bridge.py (TCP, localhost:9999) ──► PySpark Structured Streaming ──► Console / Parquet
 (ticker channel)       forwards one JSON per line          parse → clean → enrich → window
```

PySpark has no native WebSocket source, so `bridge.py` subscribes to the feed and forwards each message to a local TCP socket, which Spark reads with the built-in `socket` source.

> The socket source is intended for learning and testing because it is not fault tolerant. For production, have the bridge publish to Kafka and read it with `readStream.format("kafka")`. Only the ingestion step changes.

## Data source

- Endpoint: `wss://ws-feed.exchange.coinbase.com`
- Channel: `ticker` (public, no API key needed)
- Default products: `BTC-USD`, `ETH-USD`, `SOL-USD`

Other channels such as `level2` and `full` require authentication and are not used here.

## What the Spark job does

| Stage           | Transformation                                                                                          |
| --------------- | ------------------------------------------------------------------------------------------------------- |
| Ingest          | Reads raw JSON lines from the socket                                                                    |
| Parse and clean | `from_json` into a typed schema, keeps only `ticker` messages, drops malformed rows                 |
| Enrich          | Adds`event_time` and `spread` (best ask minus best bid)                                             |
| Analyse         | 10-second tumbling windows per product with a 30-second watermark                                       |
| Aggregate       | `ticks`, `open`, `close`, `high`, `low`, `volume`, `vwap`, `avg_spread`, `return_pct` |
| Output          | Console (update mode) or Parquet (append mode)                                                          |

## Project structure

```
.
├── bridge.py          # WebSocket -> TCP bridge
├── stream_job.py      # PySpark streaming job
├── read_results.py    # Reads the saved Parquet output
├── check_feed.py      # Optional: verifies the feed works without Spark
├── requirements.txt
├── .gitignore
└── README.md
```

## Prerequisites

- Python 3.9+
- Java 11 or 17 (`java -version` must work)
- Windows only: `winutils.exe` and `hadoop.dll` (see [Windows notes](#windows-notes))

## Setup

```bash
git clone <your-repo-url>
cd <your-repo-folder>

python -m venv .venv
# Windows PowerShell:  .\.venv\Scripts\Activate.ps1
# Linux / macOS:       source .venv/bin/activate

pip install -r requirements.txt
```

## Run

Use two terminals and **start the bridge first**.

**Terminal 1: bridge**

```bash
python bridge.py --products BTC-USD ETH-USD SOL-USD
# wait for: Waiting for Spark on 127.0.0.1:9999 ...
```

**Terminal 2: Spark job (console output)**

```bash
python stream_job.py
```

After about 15 seconds a table refreshes every 5 seconds:

```
|window_start|window_end|product_id|ticks|open|close|high|low|volume|vwap|avg_spread|return_pct|
```

Press `Ctrl+C` to stop. Console mode runs until you stop it, because a streaming query never ends on its own.

### Options

| Flag                    | Default                  | Description                          |
| ----------------------- | ------------------------ | ------------------------------------ |
| `--window`            | `10 seconds`           | Window size, for example`1 minute` |
| `--sink`              | `console`              | `console` or `parquet`           |
| `--host` / `--port` | `127.0.0.1` / `9999` | Where the Spark job connects         |

## Save results to Parquet

```bash
python stream_job.py --sink parquet
```

Append mode writes a window only after the watermark passes it, so the first files appear after about 1 to 2 minutes. Let it run for a few minutes, stop it with `Ctrl+C`, then read the data back:

```bash
python read_results.py
```

Output goes to `out/stats` (data) and `out/_chk` (checkpoint). Delete the `out` folder if you change the query and want a fresh start.

## Windows notes

Spark on Windows needs Hadoop native helpers.

1. Create `C:\hadoop\bin` and put `winutils.exe` and `hadoop.dll` in it. Use files that match your PySpark's bundled Hadoop version (check with `pip show pyspark` and the `hadoop-client-api*.jar` name in `site-packages/pyspark/jars`). A source is https://github.com/cdarlint/winutils.
2. Set these variables in every terminal that runs Spark (or set them permanently with `setx`):

```powershell
$env:HADOOP_HOME = "C:\hadoop"
$env:JAVA_HOME   = "C:\Program Files\Eclipse Adoptium\jdk-17.x.x-hotspot"
$env:Path        = "$env:HADOOP_HOME\bin;$env:JAVA_HOME\bin;$env:Path"
```

Alternatively, run the project under WSL2 (Ubuntu), where none of this is needed.

## Troubleshooting

| Problem                                                 | Fix                                                                                  |
| ------------------------------------------------------- | ------------------------------------------------------------------------------------ |
| `JAVA_GATEWAY_EXITED` / Java not found                | Install JDK 17 and set`JAVA_HOME` and `PATH`                                     |
| `HADOOP_HOME and hadoop.home.dir are unset` (Windows) | Set`HADOOP_HOME` and install `winutils.exe`                                      |
| `UnsatisfiedLinkError ... NativeIO$Windows.access0`   | `hadoop.dll` is missing, not on `PATH`, or the wrong version                     |
| Connection refused                                      | Start`bridge.py` before `stream_job.py`                                          |
| Parquet folder stays empty                              | Wait 1 to 2 minutes and check that the bridge keeps printing`forwarded N messages` |
| Checkpoint errors after editing the query               | Delete the`out` folder and restart                                                 |

## Ideas for extension

- Sliding windows: `F.window("event_time", "1 minute", "10 seconds")`
- Add the `matches` channel and compute buy versus sell volume
- Moving averages and volatility per product
- Replace the socket with Kafka for fault tolerance
- Write to Delta Lake instead of Parquet

## License

MIT (add a `LICENSE` file if you want to publish under this license).
