#!/usr/bin/env bash
# Reconcile repository milestones and issues into the shared milestone project.
# Project 8 is intentionally separate from the execution project.
# GraphQL variables are bound by gh; the dollar signs must remain literal.
# shellcheck disable=SC2016
set -euo pipefail

repo='tapstate/tapstate'
owner='tapstate'
project_number=8
progress_label='status:in-progress'

die() { printf 'milestone board: %s\n' "$*" >&2; exit 1; }
note() { printf 'milestone board: %s\n' "$*"; }

[[ ${GITHUB_REPOSITORY:-} == "$repo" ]] || die "GITHUB_REPOSITORY must be $repo"
command -v gh >/dev/null || die 'gh is required'
command -v jq >/dev/null || die 'jq is required'

project_json=$(gh api graphql -f owner="$owner" -F number="$project_number" -f query='
  query($owner:String!, $number:Int!) {
    organization(login:$owner) { projectV2(number:$number) {
      id
      field(name:"Status") {
        ... on ProjectV2SingleSelectField { id name options { id name } }
      }
    } }
  }')
project_id=$(jq -r '.data.organization.projectV2.id // empty' <<<"$project_json")
[[ -n $project_id ]] || die "cannot read organization project $owner/$project_number"
status_field_id=$(jq -r '.data.organization.projectV2.field.id // empty' <<<"$project_json")
[[ -n $status_field_id ]] || die 'project has no Status field'

status_option_id() {
  jq -r --arg name "$1" '.data.organization.projectV2.field.options[] | select(.name == $name) | .id' <<<"$project_json"
}
for status in Todo 'In Progress' Done; do
  [[ -n $(status_option_id "$status") ]] || die "project Status has no '$status' option"
done

filter_for() {
  local quoted
  quoted=$(jq -nr --arg title "$1" '$title | @json')
  printf 'repo:%s milestone:%s' "$repo" "$quoted"
}

legacy_filter_for() {
  local quoted
  quoted=$(jq -nr --arg title "$1" '$title | @json')
  printf 'milestone:%s' "$quoted"
}

project_views() {
  gh api graphql --paginate --slurp -f owner="$owner" -F number="$project_number" -f query='
    query($owner:String!, $number:Int!, $endCursor:String) {
      organization(login:$owner) { projectV2(number:$number) {
        views(first:100, after:$endCursor) {
          nodes { id name filter layout }
          pageInfo { hasNextPage endCursor }
        }
      } }
    }' | jq '[.[].data.organization.projectV2.views.nodes[] | select(.layout == "BOARD_LAYOUT")]'
}

matching_views() {
  jq --arg desired "$(filter_for "$1")" --arg legacy "$(legacy_filter_for "$1")" '
    [.[] | select((.filter // "" | sub(" +is:issue *$"; "")) as $scope
      | $scope == $desired or $scope == $legacy)]'
}

ensured_view_title=''
ensure_view() {
  local title=$1 previous=${2:-} desired views matches view_id view_name previous_filter new_filter body status_rest_id
  [[ $title != "$ensured_view_title" || -n $previous ]] || return 0
  desired=$(filter_for "$title")
  views=$(project_views)
  # A manually titled board with the same milestone scope already serves this milestone.
  if [[ $(matching_views "$title" <<<"$views" | jq 'length') -gt 0 ]]; then
    ensured_view_title=$title
    return
  fi

  if [[ -n $previous ]]; then
    matches=$(matching_views "$previous" <<<"$views")
    case $(jq 'length' <<<"$matches") in
      0) ;;
      1)
        view_id=$(jq -r '.[0].id' <<<"$matches")
        view_name=$(jq -r '.[0].name' <<<"$matches")
        previous_filter=$(jq -r '.[0].filter' <<<"$matches")
        new_filter=$desired
        if [[ $previous_filter != repo:* ]]; then
          new_filter=$(legacy_filter_for "$title")
        fi
        if [[ $previous_filter =~ [[:space:]]is:issue[[:space:]]*$ ]]; then
          new_filter+=' is:issue'
        fi
        gh api graphql -f view="$view_id" -f name="$view_name" -f filter="$new_filter" -f query='
          mutation($view:ID!, $name:String!, $filter:String!) {
            updateProjectV2View(input:{viewId:$view, name:$name, filter:$filter}) {
              projectV2View { id }
            }
          }' >/dev/null
        note "updated view scope: $title"
        ensured_view_title=$title
        return
        ;;
      *)
        note "multiple views match the previous milestone '$previous'; preserving them"
        ;;
    esac
  fi

  status_rest_id=$(gh api --paginate "orgs/$owner/projectsV2/$project_number/fields" \
    --jq '.[] | select(.name == "Status") | .id')
  [[ -n $status_rest_id ]] || die 'cannot read REST Status field ID'
  body=$(jq -n --arg name "$title" --arg filter "$desired" --argjson status "$status_rest_id" \
    '{name:$name, layout:"board", filter:$filter, vertical_group_by:[$status]}')
  printf '%s' "$body" | gh api --method POST "orgs/$owner/projectsV2/$project_number/views" --input - >/dev/null
  note "created view: $title"
  ensured_view_title=$title
}

project_item_for_issue() {
  gh api graphql --paginate --slurp -f issue="$1" -f query='
    query($issue:ID!, $endCursor:String) {
      node(id:$issue) { ... on Issue {
        projectItems(first:100, after:$endCursor) {
          nodes { id project { id } }
          pageInfo { hasNextPage endCursor }
        }
      } }
    }' | jq -r --arg project "$project_id" \
      '[.[].data.node.projectItems.nodes[] | select(.project.id == $project)][0].id // empty'
}

set_status() {
  gh api graphql -f project="$project_id" -f item="$1" \
    -f field="$status_field_id" -f option="$2" -f query='
    mutation($project:ID!, $item:ID!, $field:ID!, $option:String!) {
      updateProjectV2ItemFieldValue(input:{
        projectId:$project, itemId:$item, fieldId:$field,
        value:{singleSelectOptionId:$option}
      }) { projectV2Item { id } }
    }' >/dev/null
}

read_status() {
  gh api graphql -f item="$1" -f query='
    query($item:ID!) {
      node(id:$item) { ... on ProjectV2Item {
        fieldValueByName(name:"Status") { ... on ProjectV2ItemFieldSingleSelectValue { name } }
      } }
    }' | jq -r '.data.node.fieldValueByName.name // empty'
}

upsert_issue() {
  local issue=$1 issue_id number title state option_id result item_id attempt actual
  issue_id=$(jq -r '.node_id // empty' <<<"$issue")
  number=$(jq -r '.number // empty' <<<"$issue")
  [[ -n $issue_id && -n $number ]] || die 'issue payload lacks node_id or number'
  title=$(jq -r '.milestone.title // empty' <<<"$issue")
  # Shared projects also use label-filtered views. Milestone membership determines
  # visibility in the milestone view, so removing it needs no Project item deletion.
  [[ -n $title ]] || return 0
  state=$(jq -r --arg label "$progress_label" '
    if .state == "closed" then "Done"
    elif any(.labels[]?; .name == $label) then "In Progress"
    else "Todo" end' <<<"$issue")
  option_id=$(status_option_id "$state")
  item_id=$(project_item_for_issue "$issue_id")
  if [[ -z $item_id ]]; then
    result=$(gh api graphql -f project="$project_id" -f issue="$issue_id" -f query='
      mutation($project:ID!, $issue:ID!) {
        addProjectV2ItemById(input:{projectId:$project, contentId:$issue}) {
          item { id }
        }
      }')
    item_id=$(jq -r '.data.addProjectV2ItemById.item.id // empty' <<<"$result")
    [[ -n $item_id ]] || die "cannot add issue #$number to project"
  fi
  actual=$(read_status "$item_id")
  # The native item-added automation initializes Status. A replay of a new item can arrive before it finishes.
  if [[ $state != Todo && -z $actual ]]; then
    for attempt in 1 2 4 8 16; do
      sleep "$attempt"
      actual=$(read_status "$item_id")
      [[ -z $actual ]] || break
    done
    [[ -n $actual ]] || note "issue #$number: no native default observed; applying the requested Status"
  fi
  [[ $actual != "$state" ]] || return 0
  set_status "$item_id" "$option_id"
  for attempt in 1 2 3; do
    actual=$(read_status "$item_id")
    [[ $actual == "$state" ]] && break
    [[ $attempt == 3 ]] && die "issue #$number kept '$actual' instead of '$state'"
    sleep 1
    set_status "$item_id" "$option_id"
  done
  note "issue #$number: $state"
}

reconcile() {
  local milestone_lines milestone encoded title number issue_lines issue_encoded issue
  milestone_lines=$(gh api --paginate "repos/$repo/milestones?state=all&per_page=100" \
    --jq '.[] | @base64') || die 'cannot list milestones'
  while IFS= read -r encoded; do
    [[ -n $encoded ]] || continue
    milestone=$(printf '%s' "$encoded" | base64 -d)
    title=$(jq -r '.title' <<<"$milestone")
    number=$(jq -r '.number' <<<"$milestone")
    if [[ $(jq -r '.state' <<<"$milestone") == open ]]; then
      ensure_view "$title"
    fi
    issue_lines=$(gh api --paginate "repos/$repo/issues?state=all&milestone=$number&per_page=100" \
      --jq '.[] | select(.pull_request == null) | @base64') || die "cannot list issues for milestone $number"
    while IFS= read -r issue_encoded; do
      [[ -n $issue_encoded ]] || continue
      issue=$(printf '%s' "$issue_encoded" | base64 -d)
      upsert_issue "$issue"
    done <<<"$issue_lines"
  done <<<"$milestone_lines"

}

if [[ ${1:-} == --reconcile ]]; then
  reconcile
  exit
fi
[[ $# == 0 ]] || die "unknown argument: $1"
[[ -f ${GITHUB_EVENT_PATH:-} ]] || die 'GITHUB_EVENT_PATH is required for event mode'
event=$(<"$GITHUB_EVENT_PATH")
case ${GITHUB_EVENT_NAME:-} in
  milestone)
    action=$(jq -r '.action' <<<"$event")
    if [[ $action == deleted ]]; then
      note 'milestone deleted; ownership of existing views requires maintainer verification before removal'
      exit 0
    fi
    number=$(jq -er '.milestone.number' <<<"$event")
    milestone=$(gh api "repos/$repo/milestones/$number")
    [[ $(jq -r '.state' <<<"$milestone") == open ]] || exit 0
    title=$(jq -er '.title' <<<"$milestone")
    previous=''
    if [[ $action == edited && $title == "$(jq -r '.milestone.title' <<<"$event")" ]]; then
      previous=$(jq -r '.changes.title.from // empty' <<<"$event")
    fi
    ensure_view "$title" "$previous"
    ;;
  issues)
    issue_number=$(jq -r '.issue.number // empty' <<<"$event")
    [[ -n $issue_number ]] || die 'issue event has no issue number'
    # Read current state so an older queued webhook cannot undo a newer close or label change.
    issue=$(gh api "repos/$repo/issues/$issue_number")
    upsert_issue "$issue"
    ;;
  *) die "unsupported event: ${GITHUB_EVENT_NAME:-empty}" ;;
esac
