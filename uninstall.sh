#!/usr/bin/env bash
# uninstall.sh — DEPRECATED.  Use install.sh instead.
#
# This script is kept for backward compatibility only.
# It forwards all arguments to:   install.sh uninstall [ARGS...]
#
# The new unified script handles both installation and uninstallation:
#   sudo ./install.sh                   # install
#   sudo ./install.sh uninstall         # uninstall (equivalent to this script)
#   sudo ./install.sh --help            # full usage

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "Note: uninstall.sh is deprecated — forwarding to install.sh uninstall $*" >&2
exec "${SCRIPT_DIR}/install.sh" uninstall "$@"
