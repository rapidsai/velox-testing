#!/bin/bash

# Copyright (c) 2025, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

shopt -s extglob # needed for paramaterized case statement

function make_options() {
    local -n LOCAL_OPTION_MAP="$1"
    declare -g OPTIONS="@("
    SEPARATOR=""
    for key in "${!LOCAL_OPTION_MAP[@]}"; do
        OPTIONS+="${SEPARATOR}$key|${LOCAL_OPTION_MAP[$key]}"
        SEPARATOR="|"
    done
    OPTIONS+=")"
}

function make_flags() {
    local -n LOCAL_FLAG_MAP="$1"
    FLAGS="@("
    SEPARATOR=""
    for key in "${!LOCAL_FLAG_MAP[@]}"; do
        FLAGS+="${SEPARATOR}$key|${LOCAL_FLAG_MAP[$key]}"
        SEPARATOR="|"
    done
    FLAGS+=")"
}

function parse_option() {
    local option=$1
    [[ -v OPTION_MAP["$option"] ]] && option=${OPTION_MAP["$option"]}
    [[ $# -lt 2 || -z $2 ]] && echo "Error: $option requires a value" >&2 && exit 1
    option=${option//--/} # remove double-dash
    option=${option//-/_} # replace dash with underscore
    option=$(echo "${option^^}") # capitalize
    declare -g "$option=$2"
}

function parse_flag() {
    local flag=$1
    [[ -v FLAG_MAP["$flag"] ]] && flag=${FLAG_MAP["$flag"]}
    flag=${flag//--/} # remove double-dash
    flag=${flag//-/_} # replace dash with underscore
    flag=$(echo "${flag^^}") # capitalize
    declare -g "$flag=true"
}

parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            $OPTIONS) parse_option "$@"; shift 2;;
            $FLAGS) parse_flag $1; shift 1;;
            -h|--help) print_help; exit 0;;
            *) echo "Error: Unknown argument $1"; print_help; exit 1;;
        esac
    done
}

parse_args_no_flags() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            $OPTIONS) parse_option "$@"; shift 2;;
            -h|--help) print_help; exit 0;;
            *) echo "Error: Unknown argument $1"; print_help; exit 1;;
        esac
    done
}
