"""CLI smoke tests."""

from __future__ import annotations

from typer.testing import CliRunner

from binance_datatool.bhds.archive import CmSymbolFilter, SpotSymbolFilter, UmSymbolFilter
from binance_datatool.bhds.cli import app
from binance_datatool.bhds.workflow.archive import ListSymbolsResult
from binance_datatool.common import ContractType, SpotSymbolInfo, UmSymbolInfo

runner = CliRunner()


def test_cli_list_symbols_outputs_only_matched_symbols(monkeypatch) -> None:
    """The CLI should print only matched symbol names."""

    async def fake_run(self) -> ListSymbolsResult:
        assert self.symbol_filter is None
        return ListSymbolsResult(
            matched=[
                SpotSymbolInfo(
                    symbol="BTCUSDT",
                    base_asset="BTC",
                    quote_asset="USDT",
                    is_leverage=False,
                    is_stable_pair=False,
                ),
                SpotSymbolInfo(
                    symbol="ETHUSDT",
                    base_asset="ETH",
                    quote_asset="USDT",
                    is_leverage=False,
                    is_stable_pair=False,
                ),
            ],
            unmatched=["RAWBAD"],
            filtered_out=[
                SpotSymbolInfo(
                    symbol="BNBUPUSDT",
                    base_asset="BNBUP",
                    quote_asset="USDT",
                    is_leverage=True,
                    is_stable_pair=False,
                )
            ],
        )

    monkeypatch.setattr(
        "binance_datatool.bhds.workflow.archive.ArchiveListSymbolsWorkflow.run",
        fake_run,
    )

    result = runner.invoke(app, ["archive", "list-symbols", "spot"])

    assert result.exit_code == 0
    assert result.stdout == "BTCUSDT\nETHUSDT\n"


def test_cli_list_symbols_builds_um_filter_from_flags(monkeypatch) -> None:
    """The archive CLI should normalize repeated quote filters for USD-M."""

    async def fake_run(self) -> ListSymbolsResult:
        assert self.data_freq.value == "monthly"
        assert self.data_type.value == "fundingRate"
        assert isinstance(self.symbol_filter, UmSymbolFilter)
        assert self.symbol_filter.quote_assets == frozenset({"USDT", "USDC"})
        assert self.symbol_filter.exclude_stable_pairs is True
        assert self.symbol_filter.contract_type is ContractType.perpetual
        return ListSymbolsResult(
            matched=[
                UmSymbolInfo(
                    symbol="BTCUSDT",
                    base_asset="BTC",
                    quote_asset="USDT",
                    contract_type=ContractType.perpetual,
                    is_stable_pair=False,
                )
            ],
            unmatched=[],
            filtered_out=[],
        )

    monkeypatch.setattr(
        "binance_datatool.bhds.workflow.archive.ArchiveListSymbolsWorkflow.run",
        fake_run,
    )

    result = runner.invoke(
        app,
        [
            "archive",
            "list-symbols",
            "um",
            "--freq",
            "monthly",
            "--type",
            "fundingRate",
            "--quote",
            "usdt",
            "--quote",
            "usdc",
            "--exclude-leverage",
            "--exclude-stables",
            "--contract-type",
            "perpetual",
        ],
    )

    assert result.exit_code == 0
    assert result.stdout == "BTCUSDT\n"


def test_cli_list_symbols_ignores_inapplicable_flags(monkeypatch) -> None:
    """Flags that do not apply to the chosen market should be silently ignored."""

    async def fake_run(self) -> ListSymbolsResult:
        assert self.symbol_filter is None
        return ListSymbolsResult(matched=[], unmatched=["BTCUSDT"], filtered_out=[])

    monkeypatch.setattr(
        "binance_datatool.bhds.workflow.archive.ArchiveListSymbolsWorkflow.run",
        fake_run,
    )

    result = runner.invoke(
        app,
        [
            "archive",
            "list-symbols",
            "cm",
            "--quote",
            "usdt",
            "--exclude-leverage",
            "--exclude-stables",
        ],
    )

    assert result.exit_code == 0
    assert result.stdout == ""


def test_cli_list_symbols_builds_spot_filter(monkeypatch) -> None:
    """Spot-specific flags should produce a spot filter."""

    async def fake_run(self) -> ListSymbolsResult:
        assert isinstance(self.symbol_filter, SpotSymbolFilter)
        assert self.symbol_filter.quote_assets == frozenset({"USDT"})
        assert self.symbol_filter.exclude_leverage is True
        assert self.symbol_filter.exclude_stable_pairs is True
        return ListSymbolsResult(
            matched=[
                SpotSymbolInfo(
                    symbol="BTCUSDT",
                    base_asset="BTC",
                    quote_asset="USDT",
                    is_leverage=False,
                    is_stable_pair=False,
                )
            ],
            unmatched=[],
            filtered_out=[],
        )

    monkeypatch.setattr(
        "binance_datatool.bhds.workflow.archive.ArchiveListSymbolsWorkflow.run",
        fake_run,
    )

    result = runner.invoke(
        app,
        [
            "archive",
            "list-symbols",
            "spot",
            "--quote",
            "usdt",
            "--exclude-leverage",
            "--exclude-stables",
            "--contract-type",
            "delivery",
        ],
    )

    assert result.exit_code == 0
    assert result.stdout == "BTCUSDT\n"


def test_cli_list_symbols_builds_cm_filter(monkeypatch) -> None:
    """COIN-M should only honor the contract-type filter."""

    async def fake_run(self) -> ListSymbolsResult:
        assert isinstance(self.symbol_filter, CmSymbolFilter)
        assert self.symbol_filter.contract_type is ContractType.delivery
        return ListSymbolsResult(matched=[], unmatched=[], filtered_out=[])

    monkeypatch.setattr(
        "binance_datatool.bhds.workflow.archive.ArchiveListSymbolsWorkflow.run",
        fake_run,
    )

    result = runner.invoke(
        app,
        ["archive", "list-symbols", "cm", "--quote", "usdt", "--contract-type", "delivery"],
    )

    assert result.exit_code == 0
    assert result.stdout == ""
