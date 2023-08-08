# kind-wait

CLI app that waits for all:
- pods
- deployments
- statefulsets
- daemonsets
- nodes
- jobs

to be ready or completed. I personally use it to wait for [kind](https://kind.sigs.k8s.io/) cluster readiness, hence the name, but nothing prevents you from using it on any other cluster.

## Installation

```
go install github.com/aerfio/kind-wait@main
```

## Usage

```
kind-wait
```

That's it, it will block till all resources outlined in [the first paragraph](#kind-wait) are ready/completed.

Run it with `-h` flag to see more options, like increased verbosity.
