#!/usr/bin/env bash
set -euo pipefail
cp -n inventory.env.example inventory.env
printf 'Created inventory.env (if it did not already exist). Edit it before deployment.\n'
