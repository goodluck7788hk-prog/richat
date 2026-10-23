# Agave 4.3 message compatibility

The message and reference variants for block footers and entry parent updates
follow Shyft commit `3a14a3d69f3d0b445bbb30c99cd40c001dd2b910`.
Both parsers retain these updates as decoded Yellowstone 14.0.1 messages.
Certificate subscriptions require clients using that typed schema. Legacy
certificate bytes in reserved footer fields 6–8 are skipped during decoding.

`Message::bank_id()` returns the bank targeted by an update. Slot and account
updates preserve `None` versus `Some(0)`. Other updates use scalar bank IDs, so
legacy messages without that field read as `Some(0)`. Entry parent updates
return the cleared bank ID; `MessageEntryUpdateParent::cleared_bank_id()` is
available for explicit invalidation handling. Assembled blocks take their bank
ID from block metadata. Bank IDs must be qualified by source when joining data
from different nodes.

In addition to Shyft's interfaces, Limited slot and entry variants expose bank
IDs, every bank-bearing message has an accessor, and `ConfigLimits` supports
`block_footer.max` with the same unlimited default as other filter counts.
Entry parent filters obey entry name and count limits. Certificate inclusion
and entry parent notifications remain opt-in.

Regression tests cover complete filtered messages, bank presence and multibyte
IDs, transaction status, sliced accounts, assembled blocks, Legacy/V0/V1 wire
preservation, typed certificates, parent filters, and malformed/legacy fields.
They also cover startup flags, slot error offsets/UTF-8 validation, and emitting
each block filter name once.
