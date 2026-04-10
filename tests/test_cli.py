"""CLI smoke tests."""

from __future__ import annotations

from typer.testing import CliRunner

from binance_datatool.bhds.cli import app

runner = CliRunner()


def test_cli_list_symbols_outputs_each_symbol_on_its_own_line(monkeypatch) -> None:
    """The CLI should print one symbol per line."""

    async def fake_run(self) -> list[str]:
        return ["BTCUSDT", "ETHUSDT"]

    monkeypatch.setattr(
        "binance_datatool.bhds.workflow.archive.ArchiveListSymbolsWorkflow.run",
        fake_run,
    )

    result = runner.invoke(app, ["archive", "list-symbols", "spot"])

    assert result.exit_code == 0
    assert result.stdout == "BTCUSDT\nETHUSDT\n"


def test_cli_list_symbols_accepts_short_option_names(monkeypatch) -> None:
    """The archive CLI should expose short option names for common flags."""

    async def fake_run(self) -> list[str]:
        assert self.data_freq.value == "monthly"
        assert self.data_type.value == "fundingRate"
        return ["BTCUSDT"]

    monkeypatch.setattr(
        "binance_datatool.bhds.workflow.archive.ArchiveListSymbolsWorkflow.run",
        fake_run,
    )

    result = runner.invoke(
        app,
        ["archive", "list-symbols", "um", "--freq", "monthly", "--type", "fundingRate"],
    )

    assert result.exit_code == 0
    assert result.stdout == "BTCUSDT\n"
