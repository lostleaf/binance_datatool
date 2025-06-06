# BHDS

BHDS, short for Binance Historical Data Service, is an open-source project designed for downloading and locally maintaining historical market data from Binance. 

BHDS is primarily intended for cryptocurrency quantitative trading research using Python. It uses the open-source [Aria2](https://aria2.github.io/) downloader to retrieve historical market data, such as candlestick (K-line) and funding rates, from [Binance's official AWS historical data repository](https://data.binance.vision/). The data is then converted into a DataFrame using [Polars](https://pola.rs/) and stored in the Parquet format, facilitating efficient access for quantitative research.

This project is released under the **MIT License**.

Here's a refined version of your documentation:

## Environment

### Python Environment Setup

This project uses [uv](https://docs.astral.sh/uv/) for Python package management. To set up the environment:

1. **Install uv** (if not already installed):
   ```bash
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

2. **Create and activate a virtual environment**:
   ```bash
   uv venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. **Install project dependencies**:
   ```bash
   uv sync
   ```

### Aria2 Installation

The BHDS service requires `aria2`, an efficient cross-platform command-line download utility, for downloading historical data from Binance's AWS repository.

**Linux (Ubuntu/Debian):**
```bash
sudo apt update && sudo apt install aria2
```

**Linux (CentOS/RHEL/Fedora):**
```bash
# CentOS/RHEL
sudo yum install aria2
# or Fedora
sudo dnf install aria2
```

**macOS:**
```bash
# Using Homebrew
brew install aria2
# Using MacPorts
sudo port install aria2
```

**Windows:**
- Download the latest release from [aria2 GitHub releases](https://github.com/aria2/aria2/releases)
- Extract the archive and add the `aria2c.exe` path to your system PATH
- Alternatively, use package managers like Chocolatey: `choco install aria2`

### Configuration

By default, the BHDS service uses `$HOME/crypto_data` as the base directory, where all data is downloaded. To change this base directory, set the `CRYPTO_BASE_DIR` environment variable accordingly:

```bash
export CRYPTO_BASE_DIR=/path/to/your/crypto/data
```

## Usage

### CLI Interface

BHDS provides a command-line interface built with Python's Typer library. 
The CLI entry point is `bhds.py` and includes several command groups:

```bash
# Show help information
> python bhds.py --help

 Usage: bhds.py [OPTIONS] COMMAND [ARGS]...                                                
                                                                                           
╭─ Options ───────────────────────────────────────────────────────────────────────────────╮
│ --install-completion          Install completion for the current shell.                 │
│ --show-completion             Show completion for the current shell, to copy it or      │
│                               customize the installation.                               │
│ --help                        Show this message and exit.                               │
╰─────────────────────────────────────────────────────────────────────────────────────────╯
╭─ Commands ──────────────────────────────────────────────────────────────────────────────╮
│ aws_funding       Commands for maintaining Binance AWS funding rate data.               │
│ aws_kline         Commands for maintaining Binance AWS K-line data.                     │
│ api_data          Commands for maintaining Binance API data.                            │
│ aws_liquidation   Commands for maintaining Binance AWS liquidation snapshot data.       │
│ generate          Commands to generate the resulting data.                              │
╰─────────────────────────────────────────────────────────────────────────────────────────╯
```

Each command group has its own set of subcommands. 
Use `--help` with any command or subcommand to see its specific usage.

For example:

```bash
> python bhds.py aws_funding --help

 Usage: bhds.py aws_funding [OPTIONS] COMMAND [ARGS]...                                                        
                                                                                                               
 Commands for maintaining Binance AWS funding rate data.                                                       
                                                                                                               
╭─ Options ───────────────────────────────────────────────────────────────────────────────────────────────────╮
│ --help          Show this message and exit.                                                                 │
╰─────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
╭─ Commands ──────────────────────────────────────────────────────────────────────────────────────────────────╮
│ download              Download Binance funding rates for specific symbols from AWS data center              │
│ download-um-futures   Download Binance USDⓈ-M Futures funding rates                                         │
│ download-cm-futures   Download Binance Coin Futures funding rates                                           │
│ verify                Verify Binance funding rates for specific symbols from AWS data center                │
│ verify-type-all       Verify Binance funding rates for all symbols with the given trade type                │
│ parse                 Parse Binance funding rates for specific symbols                 │
│ parse-type-all        Parse Binance funding rates for all symbols with the given trade type                 │
╰─────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
```

```bash
> python bhds.py aws_funding download-um-futures --help

 Usage: bhds.py aws_funding download-um-futures [OPTIONS]

 Download Binance USDⓈ-M Futures funding rates

╭─ Options ───────────────────────────────────────────────────────────────────────────────────────────────────╮
│ --quote                TEXT                  The quote currency, e.g., 'USDT', 'USDC', 'BTC'.               │
│                                              [default: USDT]                                                │
│ --contract-type        [PERPETUAL|DELIVERY]  The type of contract, 'PERPETUAL' or 'DELIVERY'.               │
│                                              [default: PERPETUAL]                                           │
│ --http-proxy           TEXT                  HTTP proxy address [default: None]                             │
│ --help                                       Show this message and exit.                                    │
╰─────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
```

### Shell Scripts

BHDS provides several shell scripts for common workflows.
The following scripts should be run in sequence for a complete data pipeline:

1. `aws_download.sh`: Downloads historical 1-minute klines and funding rates data from Binance's AWS repository

2. `aws_parse.sh`: Parses the downloaded raw data and stores in parquet format

3. `api_download.sh`: Downloads missing 1-minute klines and recent funding rates data from Binance API

4. `gen_kline.sh`: Generates enhanced 1-minute klines by merging AWS and API data and joins the funding rates

5. `resample.sh` (Optional): Creates resampled timeframes
