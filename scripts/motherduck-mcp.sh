#!/usr/bin/env bash
# Starts MotherDuck's MCP server for Claude Code, pointed at the neos database.
#
# Claude Code launches MCP servers with its own environment and does not read .env, so this
# wrapper loads .env first. The token can be named MOTHERDUCK_TOKEN or MOTHERDUCK_API_KEY.
set -euo pipefail
cd "$(dirname "$0")/.."
if [ -f .env ]; then
  set -a; . ./.env; set +a
fi
export motherduck_token="${MOTHERDUCK_TOKEN:-${MOTHERDUCK_API_KEY:-}}"
if [ -z "$motherduck_token" ]; then
  echo "motherduck-mcp: set MOTHERDUCK_TOKEN in .env" >&2
  exit 1
fi
# --read-only would need a MotherDuck read-scaling token; a normal token must connect read/write.
exec uvx mcp-server-motherduck --db-path md:neos
