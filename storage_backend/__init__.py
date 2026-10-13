"""Vega storage backend — Telegram (MTProto) channels behind a ZentraGrid-shaped API.

Layout, top to bottom:

`config`      environment → channels, routes, limits
`wire`        the `ZG1|…` record codec: one document ⇄ channel messages
`query`       `where=` / `order_by=` / `limit` / `cursor`, as the clients send them
`telegram`    the only module that speaks MTProto (Telethon)
`index`       Layer 1 — the in-memory index and its cold rebuild
`documents`   Layers 2 + 3 — per-document locks, `rev` + `expected_rev`, the write path
`hotstore`    the same contract for flag collections, without a channel underneath
`files`       bytes in a channel, a pointer document in the index
`app`         the HTTP surface
"""

__version__ = "0.1.0"
