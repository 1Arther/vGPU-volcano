#!/usr/bin/env bash
set -euo pipefail

VOLCANO_REF=${VOLCANO_REF:-v1.13.1}
WORKDIR=${WORKDIR:-/home/zbs/volcano-v1.13.1-dqn}
PATCH_REPO=${PATCH_REPO:-/home/zbs/vGPU-volcano}
IMAGE=${IMAGE:-vgpu-volcano-scheduler:dqn-v8}
IMAGE_PREFIX=${IMAGE_PREFIX:-vgpu-volcano}
TAG=${TAG:-dqn-v8}

if [ ! -d "$WORKDIR/.git" ]; then
  git clone --branch "$VOLCANO_REF" https://github.com/volcano-sh/volcano.git "$WORKDIR"
fi

cd "$WORKDIR"
git fetch --tags origin "$VOLCANO_REF" || true
git checkout "$VOLCANO_REF"

rsync -a "$PATCH_REPO/pkg/" "$WORKDIR/pkg/"

if command -v go >/dev/null 2>&1; then
  make vc-scheduler
else
  echo "go is required for make vc-scheduler; install Go or run this script on a build host" >&2
  exit 1
fi

if command -v docker >/dev/null 2>&1; then
  IMAGE_PREFIX="$IMAGE_PREFIX" TAG="$TAG" make images
  docker tag "$IMAGE_PREFIX/vc-scheduler:$TAG" "$IMAGE"
elif command -v nerdctl >/dev/null 2>&1; then
  nerdctl build -t "$IMAGE" -f installer/dockerfile/scheduler/Dockerfile .
else
  echo "docker or nerdctl is required to build the scheduler image" >&2
  exit 1
fi

echo "$IMAGE"
