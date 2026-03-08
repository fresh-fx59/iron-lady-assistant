#!/bin/bash
set -euo pipefail

usage() {
  echo "Usage: $0 <instance_name> [install_path] [--share-gh-config]"
  echo "Example: $0 codex3"
  echo "Example with shared gh auth: $0 codex2 /usr/local/bin/codex2 --share-gh-config"
}

if [ $# -lt 1 ]; then
  usage
  exit 1
fi

INSTANCE_NAME="$1"
shift

INSTALL_PATH="/usr/local/bin/$INSTANCE_NAME"
SHARE_GH_CONFIG=false

while [ $# -gt 0 ]; do
  case "$1" in
    --share-gh-config)
      SHARE_GH_CONFIG=true
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      if [ "$INSTALL_PATH" = "/usr/local/bin/$INSTANCE_NAME" ]; then
        INSTALL_PATH="$1"
      else
        echo "Unknown argument: $1"
        usage
        exit 1
      fi
      ;;
  esac
  shift
done

TARGET_DIR="$HOME/.$INSTANCE_NAME"
CODEX_BIN="$(command -v codex || true)"

if [ -z "$CODEX_BIN" ]; then
  echo "codex executable not found in PATH."
  exit 1
fi

if [ -d "$TARGET_DIR" ]; then
  echo "Directory $TARGET_DIR already exists."
else
  echo "Creating directory $TARGET_DIR..."
  mkdir -p "$TARGET_DIR"
fi

if [ -f "$HOME/.gitconfig" ]; then
  echo "Symlinking .gitconfig..."
  ln -sf "$HOME/.gitconfig" "$TARGET_DIR/.gitconfig"
fi

if [ -d "$HOME/.ssh" ]; then
  echo "Symlinking .ssh..."
  ln -sf "$HOME/.ssh" "$TARGET_DIR/.ssh"
fi

if [ "$SHARE_GH_CONFIG" = true ]; then
  if [ -d "$HOME/.config/gh" ]; then
    echo "Symlinking shared gh config..."
    mkdir -p "$TARGET_DIR/.config"
    ln -sf "$HOME/.config/gh" "$TARGET_DIR/.config/gh"
  else
    echo "Skipping shared gh config because $HOME/.config/gh does not exist."
  fi
fi

echo "Installing wrapper to $INSTALL_PATH..."
sudo tee "$INSTALL_PATH" >/dev/null <<EOF
#!/bin/sh
export HOME="$TARGET_DIR"
exec "$CODEX_BIN" "\$@"
EOF
sudo chmod +x "$INSTALL_PATH"

echo "Setup complete for $INSTANCE_NAME."
echo "Wrapper installed at $INSTALL_PATH"
echo "Authenticate with: HOME=$TARGET_DIR codex login"
if [ "$SHARE_GH_CONFIG" = true ]; then
  echo "GitHub CLI auth will be shared from: $HOME/.config/gh"
else
  echo "GitHub CLI auth stays separate. If needed: HOME=$TARGET_DIR gh auth login"
fi
