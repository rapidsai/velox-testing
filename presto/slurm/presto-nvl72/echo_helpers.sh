#!/bin/bash

RED='\033[0;31m'
YELLOW='\033[1;33m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

function echo_error {
  echo -e "${RED}$1${NC}"
  exit 1
}

function echo_warning {
  echo -e "${YELLOW}$1${NC}"
}

function echo_success {
  echo -e "${GREEN}$1${NC}"
}
