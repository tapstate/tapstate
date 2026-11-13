"""Cloud dev publication must follow the actual distribution-completeness check."""

from pathlib import Path
import re
import unittest


WORKFLOW = Path(__file__).resolve().parents[1] / "workflows" / "release.yml"


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
        if re.search(r"^\s+\.github/scripts/release-manifest\.sh\s", step, re.MULTILINE)
    ]
    assert len(checks) == 1, "expected one executable distribution-completeness check"
    return checks[0]


def require_completeness_before_dev(workflow):
    jobs = jobs_of(workflow)
    holder, step = completeness_check(jobs)
    assert not re.search(r"^(?:      - |        )(?:if|continue-on-error):", step, re.MULTILINE), (
        "distribution-completeness step must not skip or tolerate failure"
    )
    assert holder in ancestors(jobs, "cloud-dev"), (
        f"distribution-completeness gate ({holder}) must succeed before Cloud dev publication"
    )
    # The v0.8.0 exception only admits an intentionally skipped gates job.
    draft_condition = re.search(r"(?m)^    if: >-\n((?:^      .+\n)+)", jobs["draft"] + "\n")
    assert draft_condition is not None
    assert " ".join(draft_condition.group(1).split()) == (
        "${{ !cancelled() && needs.version.result == 'success' "
        "&& needs.cli-native.result == 'success' && needs.server-image.result == 'success' "
        "&& (needs.gates.result == 'success' || (needs.version.outputs.skip_cloud_image_and_gates == 'true' "
        "&& needs.gates.result == 'skipped')) }}"
    ), "only the v0.8.0 gates skip may bypass default prerequisite blocking"
    assert re.search(
        r"^        if: steps.next.outputs.tag == 'v0\.8\.0' && inputs.commit == '' "
        r"&& github.ref == format\('refs/heads/\{0\}', github.event.repository.default_branch\)$",
        jobs["version"], re.MULTILINE,
    ), "the exception must be scoped to the v0.8.0 default-branch dispatch"
    for name in ancestors(jobs, "publish") | {"publish"}:
        condition = r"^    continue-on-error:" if name == "draft" else r"^    (?:if|continue-on-error):"
        assert not re.search(condition, jobs[name], re.MULTILINE), (
            f"{name} must not override failed-prerequisite blocking"
        )
    assert "cloud-dev" in ancestors(jobs, "approve")
    assert "approve" in ancestors(jobs, "publish")


class CloudDevReleaseGatesTest(unittest.TestCase):
    def setUp(self):
        self.workflow = WORKFLOW.read_text()

    def test_a_failed_distribution_check_blocks_dev_and_approval(self):
        require_completeness_before_dev(self.workflow)

    def test_removing_the_completeness_dependency_is_detected(self):
        mutated = re.sub(r"(  cloud-dev:\n.*?^    needs:) [^\n]+", r"\1 [version, gates]",
                         self.workflow, count=1, flags=re.MULTILINE | re.DOTALL)
        with self.assertRaisesRegex(AssertionError, "must succeed before Cloud dev"):
            require_completeness_before_dev(mutated)

    def test_removing_the_executable_check_is_detected(self):
        mutated = re.sub(r"^\s+\.github/scripts/release-manifest\.sh[^\n]+", "",
                         self.workflow, flags=re.MULTILINE)
        with self.assertRaisesRegex(AssertionError, "one executable distribution-completeness check"):
            require_completeness_before_dev(mutated)

    def test_bypassing_failed_prerequisites_is_detected(self):
        mutated = self.workflow.replace("  cloud-dev:\n", "  cloud-dev:\n    if: always()\n", 1)
        with self.assertRaises(AssertionError):
            require_completeness_before_dev(mutated)

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
            require_completeness_before_dev(mutated)


if __name__ == "__main__":
    unittest.main()
