#!/usr/bin/env sh
# Symlink this repo's skills into a Claude Code skills directory.
#
#   ./skills/install.sh            # link into ./.claude/skills (this repo only)
#   ./skills/install.sh --global   # link into ~/.claude/skills (any directory)
#
# Re-runnable: existing correct links are left alone; stale ones are replaced.
set -eu

skills_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)

case "${1:-}" in
  --global) target="$HOME/.claude/skills" ;;
  "")       target=$(CDPATH= cd -- "$skills_dir/.." && pwd)/.claude/skills ;;
  *)        echo "usage: $0 [--global]" >&2; exit 2 ;;
esac

mkdir -p "$target"

for skill in "$skills_dir"/*/; do
  [ -f "${skill}SKILL.md" ] || continue
  name=$(basename "$skill")
  link="$target/$name"
  target_path=${skill%/}

  if [ -L "$link" ] && [ "$(readlink "$link")" = "$target_path" ]; then
    echo "ok    $name (already linked)"
    continue
  fi

  if [ -e "$link" ] || [ -L "$link" ]; then
    rm -rf "$link"
  fi

  ln -s "$target_path" "$link"
  echo "link  $name -> $target_path"
done

echo "done: skills linked into $target"
