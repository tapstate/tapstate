"""Cloud dev publication must follow the actual distribution-completeness check."""

from pathlib import Path
import re
import unittest


WORKFLOW = Path(__file__).resolve().parents[1] / "workflows" / "release.yml"
CLI_WORKFLOW = WORKFLOW.with_name("cli-native.yml")
IMAGE_WORKFLOW = WORKFLOW.with_name("release-images.yml")
DISTRIBUTION = WORKFLOW.parents[1] / "scripts" / "prepare-release-distribution.sh"


def jobs_of(workflow):
    jobs = workflow.split("\njobs:\n", 1)[1]
    return {
        name: "\n".join(line for line in body.splitlines() if not line.lstrip().startswith("#"))
        for name, body in re.findall(
            r"^  ([\w-]+):\n(.*?)(?=^  [\w-]+:\n|\Z)", jobs, re.MULTILINE | re.DOTALL
        )
    }


def ancestors(jobs, name, visiting=()):
    assert name not in visiting, f"cyclic release dependencies: {visiting + (name,)}"
    match = re.search(r"^    needs: ([^\n]+)$", jobs[name], re.MULTILINE)
    parents = match.group(1).strip("[] ").split(",") if match else []
    result = set()
    for parent in (value.strip() for value in parents):
        assert parent in jobs, f"unknown prerequisite: {parent}"
        result.add(parent)
        result.update(ancestors(jobs, parent, visiting + (name,)))
    return result


def completeness_check(jobs):
    checks = [
        (name, step) for name, body in jobs.items()
        for step in re.split(r"(?m)(?=^      - )", body)
        if re.search(r"^        run: bash .*prepare-release-distribution\.sh\s", step, re.MULTILINE)
    ]
    assert len(checks) == 1, "expected one executable distribution-completeness check"
    return checks[0]


def require_completeness_before_dev(workflow, distribution):
    assert re.search(r"^\.github/scripts/release-manifest\.sh\s", distribution, re.MULTILINE), (
        "the distribution script must run the release manifest check"
    )
    jobs = jobs_of(workflow)
    holder, step = completeness_check(jobs)
    assert not re.search(r"^(?:      - |        )(?:if|continue-on-error):", step, re.MULTILINE), (
        "distribution-completeness step must not skip or tolerate failure"
    )
    assert holder in ancestors(jobs, "cloud-dev"), (
        f"distribution-completeness gate ({holder}) must succeed before Cloud dev publication"
    )
    for name in ancestors(jobs, "cloud-ghcr") | {"cloud-ghcr"}:
        assert not re.search(r"^    (?:if|continue-on-error):", jobs[name], re.MULTILINE), (
            f"{name} must not override failed-prerequisite blocking"
        )
    assert {"release-images", "gates"} <= ancestors(jobs, "draft")
    assert "cloud-dev" in ancestors(jobs, "approve")
    assert {"approve", "publish"} <= ancestors(jobs, "cloud-ghcr")


class CloudDevReleaseGatesTest(unittest.TestCase):
    def setUp(self):
        self.workflow = WORKFLOW.read_text()
        self.distribution = DISTRIBUTION.read_text()

    def test_a_failed_distribution_check_blocks_dev_and_approval(self):
        require_completeness_before_dev(self.workflow, self.distribution)

    def test_release_checks_have_no_temporary_exception(self):
        cli = CLI_WORKFLOW.read_text()
        for marker in ("v080-exception", "skip_release_tests", "skip-release-tests"):
            self.assertNotIn(marker, self.workflow)
            self.assertNotIn(marker, cli)
            self.assertNotIn(marker, IMAGE_WORKFLOW.read_text())

    def test_removing_the_completeness_dependency_is_detected(self):
        mutated = re.sub(r"(  cloud-dev:\n.*?^    needs:) [^\n]+", r"\1 [version, gates]",
                         self.workflow, count=1, flags=re.MULTILINE | re.DOTALL)
        with self.assertRaisesRegex(AssertionError, "must succeed before Cloud dev"):
            require_completeness_before_dev(mutated, self.distribution)

    def test_removing_the_executable_check_is_detected(self):
        mutated = re.sub(r"^        run: bash .*prepare-release-distribution\.sh[^\n]+", "",
                         self.workflow, flags=re.MULTILINE)
        with self.assertRaisesRegex(AssertionError, "one executable distribution-completeness check"):
            require_completeness_before_dev(mutated, self.distribution)

    def test_removing_the_manifest_from_the_distribution_script_is_detected(self):
        mutated = self.distribution.replace(".github/scripts/release-manifest.sh", "true")
        with self.assertRaisesRegex(AssertionError, "must run the release manifest check"):
            require_completeness_before_dev(self.workflow, mutated)

    def test_bypassing_failed_prerequisites_is_detected(self):
        mutated = self.workflow.replace("  cloud-dev:\n", "  cloud-dev:\n    if: always()\n", 1)
        with self.assertRaises(AssertionError):
            require_completeness_before_dev(mutated, self.distribution)

    def test_skipping_the_completeness_step_is_detected(self):
        self.require_step_override_rejected("if: false")

    def test_tolerating_a_failed_completeness_step_is_detected(self):
        self.require_step_override_rejected("continue-on-error: true")

    def require_step_override_rejected(self, override):
        _, step = completeness_check(jobs_of(self.workflow))
        header = step.splitlines()[0] + "\n"
        self.assertIn(header, self.workflow)
        mutated = self.workflow.replace(header, header + f"        {override}\n", 1)
        with self.assertRaisesRegex(AssertionError, "step must not skip or tolerate failure"):
            require_completeness_before_dev(mutated, self.distribution)


if __name__ == "__main__":
    unittest.main()
