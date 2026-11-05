# Maintained archive adapter for the 0.28 consumer

This narrow patch starts from upstream tag `0.28.0`, commit `59792c3d6631218fb0da174d7093c719f58f7071`.
It replaces the unmaintained `astral-tokio-tar` dependency with exactly `tar 0.4.46`, retaining the original MIT/Apache-2.0 project licenses.
The upstream [RUSTSEC-2026-0440](https://rustsec.org/advisories/RUSTSEC-2026-0440) advisory affects all versions of the former dependency; this patch adds no advisory exception.
The suggested `tar-codec` alternative was considered, but its 0.0.16 README explicitly calls it very early and not production-ready.

## Behavior

The existing async copy-to, copy-from and build-context callers remain async.
Synchronous archive/filesystem work executes only through Tokio blocking workers.
Copy-from uses 64-KiB bounded duplex bridges rather than blocking the async executor or buffering an entire download before parsing.
Copy-to retains the existing fully materialized output contract, with a cancellation-aware output buffer instead of an unnecessary extra async bridge.
Completed copy-from operations join the parser and verify the selected entry's exact decoded length before returning success.
Truncated payloads, bad checksums, source failures and target failures cannot become successful partial copies.
Cancelling an async copy-from closes its bridge endpoints, stopping further streamed reads/writes.
Cancelling copy-to closes its retained cancellation receiver, and the blocking writer rejects later writes with non-retryable `BrokenPipe` rather than a retryable interruption.
Explicit filesystem sources retain upstream caller-owned ordinary-file/directory assumptions; this is not a public untrusted-filesystem walker or a sandbox for arbitrary host paths.

Copy-from still selects only the first regular/continuous file, rejects directories and unsupported special entries, and does not unpack archive-selected paths onto the host.
The caller chooses the output path or buffer exactly as before.
Custom targets/readers are not required to become `Send` or `'static`; only bridge endpoints and owned copy-to input cross the blocking-worker boundary.
File permissions, long GNU paths, Unicode names, directory copy-in and exact byte identity remain covered behaviors.

Source read errors are recorded separately from deliberate parser-side closure of the bounded input bridge.
Rejecting an early directory in a large archive must retain `IsDirectory`, not a spurious bridge `BrokenPipe` or successful partial copy.

## Native build-context transport qualification

Actual AsyncBuilder qualification exposed intermittent BuildKit hangs at `load remote build context`, including an unchanged upstream 0.28.0 baseline that passed two repetitions but timed out on another.
The maintained and original codecs produced byte-identical 102,912-byte fixture archives, and Docker's native CLI consumed the maintained build context and returned the exact built bytes.
This failure must not be dismissed or marked passed merely because the archive itself is valid.
The build consumer now uses Bollard's public request-modifier hook on a private client clone to set `Connection: close` only for the `POST /build` request.
It preserves the existing User-Agent and separate BuildKit `/session` upgrade, without changing the shared client, replacing Bollard, changing its dependency pin or adding a custom HTTP transport.
The symptom and close-delimited workaround also have a public upstream report in [docker-modem issue 196](https://github.com/apocas/docker-modem/issues/196); that report is context, not a substitute for this fork's native repetitions.
Bounded registry-independent scratch builds exercise AsyncBuilder without remote pulls, RUN instructions or runtime execution.

## Compatibility boundary

The package version remains 0.28.0 so an application-level immutable Cargo patch can cover direct and transitive 0.28 consumers without changing their framework source revisions.
The public error variant `CopyFromContainerError::UnsupportedEntry` retains its shape, but its payload now comes from the maintained `tar::EntryType` rather than the removed crate's nominal type.
Consumers explicitly naming the removed crate's entry type must adapt; this is not a claim of universal source or binary compatibility.
UltraCareer's supported consumer APIs do not name that type and must independently pass their full locked native/Linux verification after applying the patch.
No Butter, foundation, application runtime, production deployment or provider change is included here.

## Verification scope

Codec tests exercise files/data/collections, custom modes, long Unicode paths, exact bytes beyond bridge capacity, first-file selection, non-Send targets, malformed/truncated archives, cancellation and source/target errors.
A separate real-Docker test uses the previously qualified digest-pinned Debian Bookworm image, no network, dropped capabilities and bounded memory/CPU/PIDs.
It verifies actual copy-in, copy-out to owned buffers/paths, permissions, directories and directory rejection without touching foreign containers or host data.
Native build tests separately consume the exact BuildContextBuilder archive through Docker CLI, verify built bytes in a never-started owned container, and exercise bounded AsyncBuilder completion.
Uniquely named generated images remain retained; qualification never prunes shared images or caches.
The broad upstream Docker suite includes privileged/host-namespace and unpinned/shared-tag scenarios and is not an authorization to run them as part of this patch.
Focused coverage and its limitations must remain explicit; complete application consumer verification is a separate required gate before adoption.
