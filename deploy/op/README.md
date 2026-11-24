# Tapstate OP server archive

Use a 64-bit Java 21 installation and an existing MongoDB with access to the databases you plan to
use for Tapstate. The package contains the Tapstate server, MongoDB and MongoDB Atlas connectors. It also
contains the controllers selected in the packaging workflow. Choose both Linux x86-64 and
macOS on Apple silicon, or build a single-platform archive.

1. Download the archive and its independent SHA-256 file. Verify the checksum before extracting.
   On Linux run `sha256sum -c tapstate-op-<version>.tar.gz.sha256`. On macOS use
   `shasum -a 256 -c tapstate-op-<version>.tar.gz.sha256`.
2. Extract the archive, then run `./tapstate-op/bin/linux-amd64/tapstate-ctl start` on Linux or
   `./tapstate-op/bin/darwin-arm64/tapstate-ctl start` on macOS. Use an account that owns the
   installation.
3. Open the local management address printed by the controller. Configure and test your storage in
   the browser using the bundled MongoDB connector. Save the configuration and wait for Tapstate to finish setup. Create the first administrator and sign in. The download center can be offline
   for this procedure.

The commands below use the Linux binary path. Substitute the macOS path as needed.

```sh
./tapstate-op/bin/linux-amd64/tapstate-ctl start --java-home "/path/to/Java 21" --data-dir "/path/to/Tapstate data"
./tapstate-op/bin/linux-amd64/tapstate-ctl status
./tapstate-op/bin/linux-amd64/tapstate-ctl logs --follow
./tapstate-op/bin/linux-amd64/tapstate-ctl restart
./tapstate-op/bin/linux-amd64/tapstate-ctl stop
```

By default data lives in `~/.local/share/tapstate` on Linux and
`~/Library/Application Support/tapstate` on macOS. The controller tries the explicit Java location,
the saved location, `JAVA_HOME`, then the first Java found on `PATH`, in that order. Successfully
started explicit paths are remembered for this user. After a host reboot, run `start` again.
Use `<data-dir>/conf/application.properties` for service overrides such as `server.port` (8080 by
default). The bundled default configuration lives in the archive's `conf/` directory. Use the
work directory's `connectors/` to import additional local JARs.

## Replace the package

Stop the service and wait for the controller to report that it exited. Either extract the next
archive beside the previous archive in a separate parent directory, or extract it over the same
`tapstate-op/` directory by running `tar -xzf tapstate-op-<version>.tar.gz -C <parent>`. The
parent is the directory that contains `tapstate-op/`. Run the new controller with the existing data
directory. The server and seed set are selected by the new package manifest. If extraction was
interrupted, extract the complete archive again. Existing installations and Pipeline recovery state
come from the retained data directory and MongoDB.

## Build an OP archive

In GitHub Actions, open **Build OP archive** and select **Run workflow**. Enter the Tapstate
branch or tag in `source_ref` or leave it blank to use the workflow's selected ref. Use
`refs/heads/` or `refs/tags/` to disambiguate a branch and tag with the same name. The
selected Tapstate source needs to contain the OP packaging code. Set `web_ref` to the matching
Tapstate Web ref when building a change that also needs a specific Web revision.

Choose the platform and a connector selection. **All** includes every connector listed in the
workflow. **GA**, **Beta**, and **Alpha** match the release stage declared in the selected JAR.
**Custom** uses the checked connectors. Every archive includes MongoDB and MongoDB Atlas.
Bundled JARs must be published and compatible with the selected Tapstate source.

Download the archive and its SHA-256 from the run's **Artifacts**. A branch such as
`ws/example` produces `tapstate-op-ws-example.tar.gz`. A version tag such as `v0.8.0`
produces `tapstate-op-0.8.0.tar.gz`. Other tags use the tag name. The run summary shows
the source commits and the IDs, versions, and paths of connector JARs verified in the archive.

Use the separate [Quickstart](https://github.com/tapstate/tapstate/blob/main/docs/quickstart-online.md)
to run the demo stack.
