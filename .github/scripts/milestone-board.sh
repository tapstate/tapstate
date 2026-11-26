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

error() { printf 'milestone board: %s\n' "$*" >&2; }
die() { error "$*"; exit 1; }
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
  quoted=$(jq -nr --arg title "$1" '$title | @json') || return
  printf 'repo:%s milestone:%s' "$repo" "$quoted"
}

legacy_filter_for() {
  local quoted
  quoted=$(jq -nr --arg title "$1" '$title | @json') || return
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
  local desired legacy
  desired=$(filter_for "$1") || return
  legacy=$(legacy_filter_for "$1") || return
  jq --arg desired "$desired" --arg legacy "$legacy" '
    [.[] | select((.filter // "" | sub(" +is:issue *$"; "")) as $scope
      | $scope == $desired or $scope == $legacy)]'
}

ensured_view_title=''
ensure_view() {
  local title=$1 previous=${2:-} allow_create=${3:-true}
  local desired views matches match_count view_id view_name previous_filter new_filter body status_rest_id
  [[ $title != "$ensured_view_title" || -n $previous ]] || return 0
  desired=$(filter_for "$title") || return
  views=$(project_views) || return
  # A manually titled board with the same milestone scope already serves this milestone.
  matches=$(matching_views "$title" <<<"$views") || return
  if [[ $matches != '[]' ]]; then
    ensured_view_title=$title
    return
  fi

  if [[ -n $previous ]]; then
    matches=$(matching_views "$previous" <<<"$views") || return
    match_count=$(jq 'length' <<<"$matches") || return
    case $match_count in
      0) ;;
      1)
        view_id=$(jq -r '.[0].id' <<<"$matches") || return
        view_name=$(jq -r '.[0].name' <<<"$matches") || return
        previous_filter=$(jq -r '.[0].filter' <<<"$matches") || return
        new_filter=$desired
        if [[ $previous_filter != repo:* ]]; then
          new_filter=$(legacy_filter_for "$title") || return
        fi
        if [[ $previous_filter =~ [[:space:]]is:issue[[:space:]]*$ ]]; then
          new_filter+=' is:issue'
        fi
        gh api graphql -f view="$view_id" -f name="$view_name" -f filter="$new_filter" -f query='
          mutation($view:ID!, $name:String!, $filter:String!) {
            updateProjectV2View(input:{viewId:$view, name:$name, filter:$filter}) {
              projectV2View { id }
            }
          }' >/dev/null || return
        note "updated view scope: $title"
        ensured_view_title=$title
        return
        ;;
      *)
        note "multiple views match the previous milestone '$previous'; preserving them"
        ;;
    esac
  fi

  [[ $allow_create == true ]] || return 0
  status_rest_id=$(gh api --paginate "orgs/$owner/projectsV2/$project_number/fields" \
    --jq '.[] | select(.name == "Status") | .id') || return
  if [[ -z $status_rest_id ]]; then
    error 'cannot read REST Status field ID'
    return 1
  fi
  body=$(jq -n --arg name "$title" --arg filter "$desired" --argjson status "$status_rest_id" \
    '{name:$name, layout:"board", filter:$filter, vertical_group_by:[$status]}') || return
  printf '%s' "$body" | gh api --method POST "orgs/$owner/projectsV2/$project_number/views" --input - >/dev/null || return
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
  issue_id=$(jq -r '.node_id // empty' <<<"$issue") || return
  number=$(jq -r '.number // empty' <<<"$issue") || return
  if [[ -z $issue_id || -z $number ]]; then
    error 'issue payload lacks node_id or number'
    return 1
  fi
  title=$(jq -r '.milestone.title // empty' <<<"$issue") || return
  # Shared projects also use label-filtered views. Milestone membership determines
  # visibility in the milestone view, so removing it needs no Project item deletion.
  [[ -n $title ]] || return 0
  state=$(jq -r --arg label "$progress_label" '
    if .state == "closed" then "Done"
    elif any(.labels[]?; .name == $label) then "In Progress"
    else "Todo" end' <<<"$issue") || return
  option_id=$(status_option_id "$state") || return
  item_id=$(project_item_for_issue "$issue_id") || return
  if [[ -z $item_id ]]; then
    result=$(gh api graphql -f project="$project_id" -f issue="$issue_id" -f query='
      mutation($project:ID!, $issue:ID!) {
        addProjectV2ItemById(input:{projectId:$project, contentId:$issue}) {
          item { id }
        }
      }') || return
    item_id=$(jq -r '.data.addProjectV2ItemById.item.id // empty' <<<"$result") || return
    if [[ -z $item_id ]]; then
      error "cannot add issue #$number to project"
      return 1
    fi
  fi
  actual=$(read_status "$item_id") || return
  # The native item-added automation initializes Status. A replay of a new item can arrive before it finishes.
  if [[ $state != Todo && -z $actual ]]; then
    for attempt in 1 2 4 8 16; do
      sleep "$attempt"
      actual=$(read_status "$item_id") || return
      [[ -z $actual ]] || break
    done
    [[ -n $actual ]] || note "issue #$number: no native default observed; applying the requested Status"
  fi
  [[ $actual != "$state" ]] || return 0
  set_status "$item_id" "$option_id" || return
  for attempt in 1 2 3; do
    actual=$(read_status "$item_id") || return
    [[ $actual == "$state" ]] && break
    if [[ $attempt == 3 ]]; then
      error "issue #$number kept '$actual' instead of '$state'"
      return 1
    fi
    sleep 1
    set_status "$item_id" "$option_id" || return
  done
  note "issue #$number: $state"
}

reconcile() {
  local milestone_lines milestone encoded title number state issue_lines issue_encoded issue failed=0
  milestone_lines=$(gh api --paginate "repos/$repo/milestones?state=all&per_page=100" \
    --jq '.[] | @base64') || die 'cannot list milestones'
  while IFS= read -r encoded; do
    [[ -n $encoded ]] || continue
    if ! milestone=$(printf '%s' "$encoded" | base64 -d) ||
       ! number=$(jq -er '.number' <<<"$milestone") ||
       ! title=$(jq -er '.title' <<<"$milestone") ||
       ! state=$(jq -er '.state' <<<"$milestone"); then
      error 'invalid entry in milestone listing'
      failed=1
      continue
    fi
    if [[ $state == open ]] && ! ensure_view "$title"; then
      error "milestone #$number: view sync failed"
      failed=1
    fi
    if ! issue_lines=$(gh api --paginate "repos/$repo/issues?state=all&milestone=$number&per_page=100" \
         --jq '.[] | select(.pull_request == null) | @base64'); then
      error "milestone #$number: cannot list issues"
      failed=1
      continue
    fi
    while IFS= read -r issue_encoded; do
      [[ -n $issue_encoded ]] || continue
      if ! issue=$(printf '%s' "$issue_encoded" | base64 -d); then
        error "milestone #$number: invalid entry in issue listing"
        failed=1
        continue
      fi
      if ! upsert_issue "$issue"; then
        error "milestone #$number: issue #$(jq -r '.number // "unknown"' <<<"$issue") failed to sync"
        failed=1
      fi
    done <<<"$issue_lines"
  done <<<"$milestone_lines"
  return "$failed"
}

read_event_resource() {
  local path=$1 response status separator=$'\r\n\r\n'
  if response=$(gh api --include "$path"); then
    if [[ $response != *"$separator"* ]]; then
      error "invalid HTTP response for $path"
      return 1
    fi
    printf '%s\n' "${response#*"$separator"}"
    return
  fi
  status=${response%%$'\n'*}
  [[ $status =~ ^HTTP/[^[:space:]]+[[:space:]]404([[:space:]]|$) ]]
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
    milestone=$(read_event_resource "repos/$repo/milestones/$number") || die "cannot read milestone #$number"
    if [[ -z $milestone ]]; then
      note "milestone #$number no longer exists"
      exit 0
    fi
    state=$(jq -er '.state' <<<"$milestone")
    title=$(jq -er '.title' <<<"$milestone")
    previous=''
    if [[ $action == edited && $title == "$(jq -r '.milestone.title' <<<"$event")" ]]; then
      previous=$(jq -r '.changes.title.from // empty' <<<"$event")
    fi
    if [[ $state == open ]]; then
      ensure_view "$title" "$previous"
    elif [[ -n $previous ]]; then
      ensure_view "$title" "$previous" false
    fi
    ;;
  issues)
    issue_number=$(jq -r '.issue.number // empty' <<<"$event")
    [[ -n $issue_number ]] || die 'issue event has no issue number'
    # Read current state so an older queued webhook cannot undo a newer close or label change.
    issue=$(read_event_resource "repos/$repo/issues/$issue_number") || die "cannot read issue #$issue_number"
    if [[ -z $issue ]]; then
      note "issue #$issue_number no longer exists"
      exit 0
    fi
    upsert_issue "$issue"
    ;;
  *) die "unsupported event: ${GITHUB_EVENT_NAME:-empty}" ;;
esac
