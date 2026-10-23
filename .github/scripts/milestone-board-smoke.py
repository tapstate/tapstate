#!/usr/bin/env python3
"""Exercise milestone events and reconciliation against a stateful GitHub API stub."""

import json
import os
import subprocess
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parent
SCRIPT = ROOT / "milestone-board.sh"
MOCK_GH = r'''#!/usr/bin/env python3
import base64
import json
import os
import sys
from pathlib import Path

path = Path(os.environ["MOCK_GH_STATE"])
state = json.loads(path.read_text())
args = sys.argv[1:]
assert args[0] == "api", args
assert "projectsV2/1" not in " ".join(args), args
params = {}
for index, word in enumerate(args):
    if word in ("-f", "-F"):
        key, value = args[index + 1].split("=", 1)
        params[key] = value
assert params.get("number") != "1", args

def save():
    path.write_text(json.dumps(state))

def answer(value):
    print(json.dumps(value))
    save()
    sys.exit(0)

endpoint = "graphql" if "graphql" in args else next(
    (word for word in args if word.startswith("orgs/") or word.startswith("repos/")), ""
)
if endpoint == "graphql":
    query = params.get("query", "")
    if "fields(first:50)" in query:
        answer({"data": {"organization": {"projectV2": {
            "id": "PVT8", "fields": {"nodes": [{"id": "STATUS_FIELD", "name": "Status",
            "options": [{"id": "TODO", "name": "Todo"},
                        {"id": "PROGRESS", "name": "In Progress"},
                        {"id": "DONE", "name": "Done"}]}]}
        }}}})
    if "views(first:100)" in query:
        answer({"data": {"organization": {"projectV2": {
            "views": {"nodes": state["views"]}
        }}}})
    if "updateProjectV2View" in query:
        view = next(view for view in state["views"] if view["id"] == params["view"])
        view["name"] = params["name"]
        view["filter"] = params["filter"]
        answer({"data": {"updateProjectV2View": {"projectV2View": {"id": view["id"]}}}})
    if "projectItems(first:100)" in query:
        item = state["items"].get(params["issue"])
        nodes = [{"id": item["id"], "project": {"id": "PVT8"}}] if item else []
        answer({"data": {"node": {"projectItems": {"nodes": nodes}}}})
    if "fieldValues(first:50)" in query:
        item = next(value for value in state["items"].values() if value["id"] == params["item"])
        if item.get("reset_once") and item["status"] != "TODO":
            item["status"] = "TODO"
            item["reset_once"] = False
        names = {"TODO": "Todo", "PROGRESS": "In Progress", "DONE": "Done"}
        nodes = [{"name": names[item["status"]], "field": {"name": "Status"}}] if item["status"] else []
        answer({"data": {"node": {"fieldValues": {"nodes": nodes}}}})
    if "addProjectV2ItemById" in query:
        issue = params["issue"]
        if issue not in state["items"]:
            state["items"][issue] = {"id": "ITEM-" + issue, "status": None, "reset_once": True}
        answer({"data": {"addProjectV2ItemById": {
            "item": {"id": state["items"][issue]["id"]}
        }}})
    if "updateProjectV2ItemFieldValue" in query:
        assert params["project"] == "PVT8" and params["field"] == "STATUS_FIELD"
        item = next(value for value in state["items"].values() if value["id"] == params["item"])
        item["status"] = params["option"]
        answer({"data": {"updateProjectV2ItemFieldValue": {
            "projectV2Item": {"id": item["id"]}
        }}})
    if "deleteProjectV2Item" in query:
        assert params["project"] == "PVT8"
        issue = next(key for key, value in state["items"].items() if value["id"] == params["item"])
        del state["items"][issue]
        answer({"data": {"deleteProjectV2Item": {"deletedItemId": params["item"]}}})
    if "items(first:100" in query:
        nodes = []
        for issue_id, item in state["items"].items():
            issue = state["issues"][issue_id]
            nodes.append({"id": item["id"], "content": {
                "repository": {"nameWithOwner": "tapstate/tapstate"},
                "milestone": issue.get("milestone")
            }})
        answer({"data": {"organization": {"projectV2": {"items": {
            "nodes": nodes, "pageInfo": {"hasNextPage": False, "endCursor": None}
        }}}}})
    raise AssertionError("Unhandled GraphQL call: " + query)

if endpoint == "orgs/tapstate/projectsV2/8/fields":
    print("427213889")
    sys.exit(0)
if endpoint == "orgs/tapstate/projectsV2/8/views":
    body = json.load(sys.stdin)
    assert body["layout"] == "board"
    assert body["vertical_group_by"] == [427213889]
    view = {"id": "VIEW-" + str(max(int(view["id"].split("-")[1]) for view in state["views"]) + 1),
            "name": body["name"], "filter": body["filter"]}
    state["views"].append(view)
    answer({"value": view})
if endpoint.startswith("repos/tapstate/tapstate/issues/"):
    number = endpoint.rsplit("/", 1)[1]
    answer(state["issues"]["ISSUE-" + number])
if endpoint.startswith("repos/tapstate/tapstate/milestones?"):
    if os.environ.get("MOCK_GH_FAIL_MILESTONES") == "1":
        sys.exit(12)
    for milestone in state["milestones"]:
        print(base64.b64encode(json.dumps(milestone).encode()).decode())
    sys.exit(0)
if endpoint.startswith("repos/tapstate/tapstate/issues?"):
    number = endpoint.split("milestone=")[1].split("&")[0]
    for issue in state["issues"].values():
        if (issue.get("milestone") or {}).get("number") == int(number):
            print(base64.b64encode(json.dumps(issue).encode()).decode())
    sys.exit(0)
raise AssertionError("Unhandled API call: " + repr(args))
'''


def issue(number, milestone, state="open", labels=None):
    return {
        "node_id": f"ISSUE-{number}",
        "number": number,
        "state": state,
        "labels": [{"name": name} for name in (labels or [])],
        "milestone": milestone,
    }


def run():
    with tempfile.TemporaryDirectory() as tmp:
        directory = Path(tmp)
        mock = directory / "gh"
        mock.write_text(MOCK_GH)
        mock.chmod(0o755)
        state_file = directory / "state.json"
        week4 = {"number": 5, "title": "Week 4 — MVP lifecycle"}
        week8 = {"number": 6, "title": "Week 8 — First customer"}
        week12 = {"number": 7, "title": "Week 12 — Paid pilot"}
        state_file.write_text(json.dumps({
            "views": [{"id": "VIEW-2", "name": "Week 4 Board",
                       "filter": 'milestone:"Week 4 — MVP lifecycle"'}],
            "items": {"ISSUE-576": {"id": "ITEM-ISSUE-576", "status": "PROGRESS"},
                      "ISSUE-999": {"id": "ITEM-ISSUE-999", "status": "TODO"}},
            "milestones": [week4, week8, week12],
            "issues": {"ISSUE-576": issue(576, week4, labels=["status:in-progress"]),
                       "ISSUE-901": issue(901, week12, state="closed"),
                       "ISSUE-999": issue(999, None)},
        }))
        env = dict(os.environ, GITHUB_REPOSITORY="tapstate/tapstate",
                   MOCK_GH_STATE=str(state_file), PATH=f"{directory}:{os.environ['PATH']}")
        event_file = directory / "event.json"

        def invoke(event_name, payload, refresh_issue=True):
            if event_name == "issues" and refresh_issue:
                current_state = json.loads(state_file.read_text())
                current_issue = payload["issue"]
                current_state["issues"][current_issue["node_id"]] = current_issue
                state_file.write_text(json.dumps(current_state))
            event_file.write_text(json.dumps(payload))
            result = subprocess.run([str(SCRIPT)], env=dict(env, GITHUB_EVENT_NAME=event_name,
                                    GITHUB_EVENT_PATH=str(event_file)), capture_output=True,
                                    text=True, check=False)
            assert result.returncode == 0, result.stdout + result.stderr
            return json.loads(state_file.read_text())

        state = invoke("milestone", {"milestone": week4})
        assert len(state["views"]) == 1
        assert state["views"][0]["id"] == "VIEW-2"
        assert state["views"][0]["filter"] == 'repo:tapstate/tapstate milestone:"Week 4 — MVP lifecycle"'

        state = invoke("milestone", {"milestone": week8})
        state = invoke("milestone", {"milestone": week8})
        assert len(state["views"]) == 2, "a replay must not create a second view"
        previous = week8["title"]
        week8 = dict(week8, title="Week 8 revised")
        state = invoke("milestone", {"milestone": week8, "changes": {"title": {"from": previous}}})
        assert len(state["views"]) == 2
        assert state["views"][1]["filter"] == 'repo:tapstate/tapstate milestone:"Week 8 revised"', state["views"]

        current = issue(900, week8)
        state = invoke("issues", {"issue": current})
        assert state["items"]["ISSUE-900"]["status"] == "TODO"
        current = issue(900, week8, labels=["status:in-progress"])
        state = invoke("issues", {"issue": current})
        assert state["items"]["ISSUE-900"]["status"] == "PROGRESS"
        current = issue(900, week8, state="closed", labels=["status:in-progress"])
        state = invoke("issues", {"issue": current})
        assert state["items"]["ISSUE-900"]["status"] == "DONE"
        current = issue(900, week8, labels=["status:in-progress"])
        state = invoke("issues", {"issue": current})
        assert state["items"]["ISSUE-900"]["status"] == "PROGRESS"
        stale = issue(900, week8, state="closed")
        state = invoke("issues", {"issue": stale}, refresh_issue=False)
        assert state["items"]["ISSUE-900"]["status"] == "PROGRESS", "old events must read current issue state"
        state = invoke("issues", {"issue": issue(900, None)})
        assert "ISSUE-900" not in state["items"]

        result = subprocess.run([str(SCRIPT), "--reconcile"], env=env, capture_output=True,
                                text=True, check=False)
        assert result.returncode == 0, result.stdout + result.stderr
        state = json.loads(state_file.read_text())
        assert any(view["name"] == week12["title"] for view in state["views"])
        assert state["items"]["ISSUE-901"]["status"] == "DONE"
        assert state["items"]["ISSUE-576"]["status"] == "PROGRESS"
        assert "ISSUE-999" not in state["items"], "reconcile must remove stale items"
        assert len([view for view in state["views"] if view["name"] == week4["title"]]) == 1
        failure = subprocess.run([str(SCRIPT), "--reconcile"],
                                 env=dict(env, MOCK_GH_FAIL_MILESTONES="1"),
                                 capture_output=True, text=True, check=False)
        assert failure.returncode != 0 and "cannot list milestones" in failure.stderr
        print("milestone board smoke: PASS")


if __name__ == "__main__":
    run()
