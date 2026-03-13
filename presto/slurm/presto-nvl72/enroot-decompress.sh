#!/bin/bash
# Auto-detecting decompressor for enroot image layer downloads.
# Used as ENROOT_GZIP_PROGRAM to support both gzip and OCI tar+zstd layers.
# Called by enroot as: enroot-decompress.sh -d -f -c  (args are ignored)

tmp=$(mktemp)
trap 'rm -f "$tmp"' EXIT

# Peek at first 4 bytes to detect compression format without buffering the full stream
dd bs=1 count=4 2>/dev/null > "$tmp"
magic=$(od -A n -N 4 -t x1 "$tmp" | tr -d ' \n')

case "$magic" in
    1f8b*)
        # gzip
        { cat "$tmp"; cat; } | gzip -d -f -c
        ;;
    28b52ffd*)
        # zstd (magic: 0xFD2FB528 stored little-endian = 28 b5 2f fd)
        { cat "$tmp"; cat; } | zstd -d -f -c
        ;;
    *)
        # Unknown format — pass through unchanged
        cat "$tmp"
        cat
        ;;
esac
