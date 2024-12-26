def find_kline_missing_dts(trade_type: TradeType, time_interval: str, symbol: str, gaps: list[tuple[str]]):
    gaps = [(date_parser.parse(st).date(), date_parser.parse(en).date()) for st, en in gaps]

    base_dir_tokens = get_kline_path_tokens(trade_type)
    kline_dir = Config.BINANCE_DATA_DIR / 'aws_data' / Path(get_aws_dir(base_dir_tokens))
    symbol_kline_dir = kline_dir / symbol / time_interval

    zip_files = symbol_kline_dir.glob('*.zip')
    filenames = [f.stem for f in zip_files]
    dts_download = {date_parser.parse(fn.split('-', 2)[-1]).date() for fn in filenames}

    dt_start = min(dts_download)
    dt_end = max(dts_download)
    dt_range = pl.date_range(dt_start, dt_end, '1d', eager=True)

    dts_missing = set(dt_range) - dts_download

    dts_missing = sorted(dt for dt in dts_missing if not any(st_gap <= dt <= en_gap for st_gap, en_gap in gaps))

    return dts_missing


def find_kline_missing_dts_all_symbols(trade_type: TradeType, time_interval: str):
    symbols = list_local_kline_symbols(trade_type, time_interval)
    symbol_dts_missing = dict()

    gap_file = Config.BHDS_KLINE_GAPS_DIR / f'{trade_type.value}.json'
    gap_data = dict()
    if gap_file.exists():
        with open(gap_file, 'r') as fin:
            gap_data = json.load(fin)

    for symbol in symbols:
        gaps = gap_data.get(symbol, [])
        dts_missing = find_kline_missing_dts(trade_type, time_interval, symbol, gaps)
        if dts_missing:
            symbol_dts_missing[symbol] = dts_missing

    return symbol_dts_missing
