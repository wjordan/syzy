# Syzy lazy restore

`lazyrestore` prepares an object-backed Syzy database as a sparse file and
exposes it through a FUSE mount that fetches missing SQLite pages on demand.

The sparse backing file must not be opened directly. Keep it outside the
application-visible filesystem and expose only the mounted path.

This package is Linux-specific. Applications that do not need lazy page loading
should use `syzy.Restore` or `syzy.RestoreFromBucket` from the SQLite package.
