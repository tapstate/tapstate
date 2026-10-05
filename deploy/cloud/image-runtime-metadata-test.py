#!/usr/bin/env python3
"""Small ZIPs exercise input binding only, never runtime or connector execution."""
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
import zipfile

SPEC = importlib.util.spec_from_file_location("runtime_metadata", Path(__file__).with_name("image-runtime-metadata.py"))
assert SPEC is not None and SPEC.loader is not None
SUBJECT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SUBJECT)


class MetadataTest(unittest.TestCase):
    def setUp(self):
        self.owned = tempfile.TemporaryDirectory(prefix="runtime-metadata-test-")
        self.addCleanup(self.owned.cleanup)
        self.root = Path(self.owned.name)
        self.lock = self.root / "lock.json"
        self.lock.write_text(json.dumps({"connectors": [{"id": "fixture-only"}]}))
        # Only this metadata unit substitutes SDK bytes. The real container suite never does.
        self.sdk_bytes = b"metadata-unit-only-sdk"
        original = SUBJECT.SDK_SHA
        SUBJECT.SDK_SHA = hashlib.sha256(self.sdk_bytes).hexdigest()
        self.addCleanup(setattr, SUBJECT, "SDK_SHA", original)

    def inputs(self, profile):
        content = f"<script src=\"/assets/{profile}.js\"></script>".encode()
        asset = f"fixture:{profile}".encode()
        files = {"index.html": content, f"assets/{profile}.js": asset}
        manifest = "".join(f"{hashlib.sha256(value).hexdigest()}  ./{name}\n"
                           for name, value in sorted(files.items())).encode()
        props = {"release.version": "0.6.0", "tapstate.revision": "a" * 40,
                 "revision": "b" * 40, "files.sha256": hashlib.sha256(manifest).hexdigest(),
                 "web.profile": profile, "repository": "tapstate/tapstate-web"}
        if profile == "cloud":
            props["cloud.console.url"] = "https://console.example.test/"
        jar = self.root / f"{profile}.jar"
        with zipfile.ZipFile(jar, "w") as archive:
            archive.writestr("META-INF/tapstate-web.properties", "".join(f"{key}={value}\n" for key, value in props.items()))
            archive.writestr("META-INF/tapstate-web.files.sha256", manifest)
            archive.writestr(SUBJECT.SDK, self.sdk_bytes)
            archive.writestr("META-INF/tapstate/connectors.lock.json", self.lock.read_bytes())
            for name, value in files.items():
                archive.writestr("BOOT-INF/classes/static/" + name, value)
        labels = {"org.opencontainers.image.version": props["release.version"],
                  "org.opencontainers.image.revision": props["tapstate.revision"],
                  "io.tapstate.web.revision": props["revision"],
                  "io.tapstate.web.files.sha256": props["files.sha256"],
                  "io.tapstate.web.profile": profile, "io.tapstate.distribution": profile}
        env = []
        if profile == "cloud":
            labels["io.tapstate.web.cloud-console-url"] = props["cloud.console.url"]
            env = ["TAPSTATE_CONNECTORS_SEED_DIR=/opt/tapstate/connectors"]
        image = self.root / f"{profile}-image.json"
        image.write_text(json.dumps([{"Id": "sha256:" + ("c" if profile == "cloud" else "d") * 64,
                                     "Config": {"Labels": labels, "Env": env, "User": "tapstate",
                                                "Entrypoint": ["java", "-jar", "/opt/tapstate/tapstate.jar"],
                                                "Healthcheck": {"Test": ["CMD-SHELL", "curl"]}}}]))
        return jar, image

    def test_the_distinct_pair_is_bound_to_the_packaged_files(self):
        cloud = SUBJECT.metadata(*self.inputs("cloud"), "cloud", self.lock)
        onprem = SUBJECT.metadata(*self.inputs("onprem"), "onprem", self.lock)
        SUBJECT.paired(cloud, onprem)
        self.assertEqual(cloud["webFiles"]["index.html"], hashlib.sha256(b'<script src="/assets/cloud.js"></script>').hexdigest())

    def test_crossed_web_profile_is_rejected(self):
        with self.assertRaises(ValueError):
            SUBJECT.metadata(*self.inputs("cloud"), "onprem", self.lock)

    def test_bad_image_labels_and_entrypoint_are_rejected(self):
        for change in ["profile", "entrypoint", "deployment-secret", "seed", "healthcheck"]:
            with self.subTest(change=change):
                jar, path = self.inputs("cloud")
                image = json.loads(path.read_text())
                config = image[0]["Config"]
                if change == "profile":
                    config["Labels"]["io.tapstate.web.profile"] = "onprem"
                elif change == "entrypoint":
                    config["Entrypoint"] = ["sh"]
                elif change == "deployment-secret":
                    config["Env"].append("TAPSTATE_CLOUD_TOKEN=must-not-be-baked-in")
                elif change == "seed":
                    config["Env"] = []
                else:
                    config["Healthcheck"] = {}
                path.write_text(json.dumps(image))
                with self.assertRaises((ValueError, SUBJECT.PROVENANCE.ProvenanceError)):
                    SUBJECT.metadata(jar, path, "cloud", self.lock)

    def test_stale_sdk_and_external_lock_are_rejected(self):
        jar, path = self.inputs("cloud")
        SUBJECT.SDK_SHA = "0" * 64
        with self.assertRaises(ValueError):
            SUBJECT.metadata(jar, path, "cloud", self.lock)
        SUBJECT.SDK_SHA = hashlib.sha256(self.sdk_bytes).hexdigest()
        self.lock.write_text('{"connectors": []}')
        with self.assertRaises(ValueError):
            SUBJECT.metadata(jar, path, "cloud", self.lock)

    def test_same_bytes_and_mixed_revisions_cannot_be_reported_as_a_pair(self):
        cloud = SUBJECT.metadata(*self.inputs("cloud"), "cloud", self.lock)
        onprem = SUBJECT.metadata(*self.inputs("onprem"), "onprem", self.lock)
        for key in ["imageId", "bootJarSha256", "webFilesSha256", "version", "tapstateRevision", "webRevision"]:
            with self.subTest(key=key):
                crossed = dict(onprem)
                crossed[key] = cloud[key] if key in ["imageId", "bootJarSha256", "webFilesSha256"] else "wrong"
                with self.assertRaises(ValueError):
                    SUBJECT.paired(cloud, crossed)


if __name__ == "__main__":
    unittest.main()
