# kubeadp

Kubernetes-native agent platform. This repository holds two parts:

- **The kubeadp operator** (Go, `api/`, `cmd/`, `internal/`, `charts/`): a fork of
  [kubeopencode/kubeopencode](https://github.com/kubeopencode/kubeopencode) (MIT), the base of the platform. The
  upstream project description is the [README](../README.md).
- **kubeadp-agent** (Rust, [`src/kubeadp-agent`](src/kubeadp-agent)): the agent runtime and orchestrator (job
  engine, sessions, authenticated agent-to-agent messaging, the `agent-run` CLI). Its normative specification is
  [`SPEC.md`](src/kubeadp-agent/SPEC.md); user and developer docs are in [`docs/`](src/kubeadp-agent/docs).
  Releases use calendar versions (`kubeadp-agent-YYYY.MM.N`) and publish the image
  `ghcr.io/cjblackout/kubeopencode/kubeadp-agent`.
