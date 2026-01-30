#!/usr/bin/env bash
awk -F'|' '
  /^[[:space:]]*Q[0-9]+/ {
    v = $NF
    gsub(/^[[:space:]]+|[[:space:]]+$/, "", v)
    up = toupper(v)
    if (up == "NULL" || v == "") { print "NULL"; next }
    if (v ~ /^-?[0-9]+(\.[0-9]+)?$/) { printf "%.3f\n", v/1000 }
  }
' "$@"
