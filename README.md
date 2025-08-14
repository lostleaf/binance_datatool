# BHDS

BHDS, short for Binance Historical Data Service, is an open-source project designed for downloading and locally maintaining historical market data from Binance. 

BHDS is primarily intended for cryptocurrency quantitative trading research using Python. It uses the open-source [Aria2](https://aria2.github.io/) downloader to retrieve historical market data, such as candlestick (K-line) and funding rates, from [Binance's official AWS historical data repository](https://data.binance.vision/). The data is then converted into a DataFrame using [Polars](https://pola.rs/) and stored in the Parquet format, facilitating efficient access for quantitative research.

This project is released under the **MIT License**.

**Be careful, I'm refactoring this project, the code may be broken.**

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
sudo apt install aria2
```

**Linux (CentOS/RHEL/Fedora):**
```bash
sudo yum install aria2
```

**macOS:**
```bash
# Using Homebrew
brew install aria2
```

