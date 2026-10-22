"""TAP_SCHEMA discovery fallbacks."""

from __future__ import annotations

import polars as pl

from xmatcher import discovery


def test_discover_columns_falls_back_when_schema_name_is_unsupported(monkeypatch):
    """Data Lab's TAP_SCHEMA has no ``schema_name`` column.

    Splitting ``nsc_dr2.object`` into schema/table and filtering on
    ``schema_name`` made the server reject the query, so ``discover
    --schema`` failed. The fallback must query the dotted id instead.
    """
    queries: list[str] = []

    def fake_execute(service, query):
        queries.append(query)
        if "schema_name" in query:
            raise RuntimeError("Column [schema_name] does not exist.")
        return query

    def fake_convert(query):
        if "table_name = 'nsc_dr2.object'" in query:
            return pl.DataFrame({"column_name": ['"ra"', "dec"]})
        return pl.DataFrame(schema={"column_name": pl.Utf8})

    monkeypatch.setattr(discovery, "execute_tap_query", fake_execute)
    monkeypatch.setattr(discovery, "get_tap_service", lambda url, auth_session=None: object())
    monkeypatch.setattr("xmatcher.io_utils.astropy_table_to_polars", fake_convert)

    df = discovery.discover_columns("https://example.org/tap", "nsc_dr2.object")

    assert df["column_name"].to_list() == ["ra", "dec"]
    assert any("table_name = 'nsc_dr2.object'" in query for query in queries)
    assert "schema_name" in queries[0]
