#!/bin/bash

if [ -z "$1" ]; then
  echo "Usage: $0 <instance_name>"
  echo "Example: $0 codex3"
  exit 1
fi

INSTANCE_NAME="$1"
TARGET_DIR="$HOME/.$INSTANCE_NAME"
ALIAS_NAME="$INSTANCE_NAME"

# Check if directory already exists
if [ -d "$TARGET_DIR" ]; then
  echo "Directory $TARGET_DIR already exists."
else
  echo "Creating directory $TARGET_DIR..."
  mkdir -p "$TARGET_DIR"
fi

# Symlink .gitconfig and .ssh if they exist in HOME
if [ -f "$HOME/.gitconfig" ]; then
  echo "Symlinking .gitconfig..."
  ln -sf "$HOME/.gitconfig" "$TARGET_DIR/.gitconfig"
fi

if [ -d "$HOME/.ssh" ]; then
  echo "Symlinking .ssh..."
  ln -sf "$HOME/.ssh" "$TARGET_DIR/.ssh"
fi

# Add alias to .bashrc if it doesn't exist
if grep -q "alias $ALIAS_NAME=" "$HOME/.bashrc"; then
  echo "Alias '$ALIAS_NAME' already exists in .bashrc."
else
  echo "Adding alias '$ALIAS_NAME' to .bashrc..."
  echo "alias $ALIAS_NAME='HOME=$TARGET_DIR codex'" >> "$HOME/.bashrc"
fi

echo "Setup complete for $INSTANCE_NAME."
echo "Please run 'source ~/.bashrc' to activate the alias."
