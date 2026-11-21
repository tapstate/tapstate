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
      fields(first:50) { nodes {
        ... on ProjectV2SingleSelectField { id name options { id name } }
      } }
    } }
  }')
project_id=$(jq -r '.data.organization.projectV2.id // empty' <<<"$project_json")
[[ -n $project_id ]] || die "cannot read organization project $owner/$project_number"
status_field_id=$(jq -r '.data.organization.projectV2.fields.nodes[] | select(.name == "Status") | .id' <<<"$project_json")
[[ -n $status_field_id ]] || die 'project has no Status field'

status_option_id() {
  jq -r --arg name "$1" '.data.organization.projectV2.fields.nodes[]
    | select(.name == "Status") | .options[] | select(.name == $name) | .id' <<<"$project_json"
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
  gh api graphql -f owner="$owner" -F number="$project_number" -f query='
    query($owner:String!, $number:Int!) {
      organization(login:$owner) { projectV2(number:$number) {
        views(first:100) { nodes { id name filter } }
      } }
    }'
}

ensured_view_title=''
ensure_view() {
  local title=$1 previous=${2:-} desired legacy previous_filter previous_legacy views view_id current_name current_filter body status_rest_id
  [[ $title != "$ensured_view_title" || -n $previous ]] || return 0
  desired=$(filter_for "$title")
  legacy=$(legacy_filter_for "$title")
  previous_filter=''
  previous_legacy=''
  if [[ -n $previous ]]; then
    previous_filter=$(filter_for "$previous")
    previous_legacy=$(legacy_filter_for "$previous")
  fi
  views=$(project_views)
  view_id=$(jq -r --arg desired "$desired" --arg legacy "$legacy" \
    --arg previous "$previous_filter" --arg previous_legacy "$previous_legacy" \
    '[.data.organization.projectV2.views.nodes[]
      | select(.filter == $desired or .filter == $legacy or
               ($previous != "" and .filter == $previous) or
               ($previous_legacy != "" and .filter == $previous_legacy))][0].id // empty' <<<"$views")
  if [[ -n $view_id ]]; then
    current_name=$(jq -r --arg id "$view_id" '.data.organization.projectV2.views.nodes[] | select(.id == $id) | .name' <<<"$views")
    current_filter=$(jq -r --arg id "$view_id" '.data.organization.projectV2.views.nodes[] | select(.id == $id) | .filter' <<<"$views")
    if [[ $current_name != "$title" || $current_filter != "$desired" ]]; then
      gh api graphql -f view="$view_id" -f name="$title" -f filter="$desired" -f query='
        mutation($view:ID!, $name:String!, $filter:String!) {
          updateProjectV2View(input:{viewId:$view, name:$name, filter:$filter}) {
            projectV2View { id }
          }
        }' >/dev/null
      note "updated view: $title"
    fi
    ensured_view_title=$title
    return
  fi

  status_rest_id=$(gh api "orgs/$owner/projectsV2/$project_number/fields" \
    --jq '.[] | select(.name == "Status") | .id')
  [[ -n $status_rest_id ]] || die 'cannot read REST Status field ID'
  body=$(jq -n --arg name "$title" --arg filter "$desired" --argjson status "$status_rest_id" \
    '{name:$name, layout:"board", filter:$filter, vertical_group_by:[$status]}')
  printf '%s' "$body" | gh api --method POST "orgs/$owner/projectsV2/$project_number/views" --input - >/dev/null
  note "created view: $title"
  ensured_view_title=$title
}

project_item_for_issue() {
  gh api graphql -f issue="$1" -f query='
    query($issue:ID!) {
      node(id:$issue) { ... on Issue {
        projectItems(first:100) { nodes { id project { id } } }
      } }
    }' | jq -r --arg project "$project_id" \
      '[.data.node.projectItems.nodes[] | select(.project.id == $project)][0].id // empty'
}

remove_issue() {
  local issue_id=$1 item_id
  item_id=$(project_item_for_issue "$issue_id")
  [[ -n $item_id ]] || return 0
  gh api graphql -f project="$project_id" -f item="$item_id" -f query='
    mutation($project:ID!, $item:ID!) {
      deleteProjectV2Item(input:{projectId:$project, itemId:$item}) { deletedItemId }
    }' >/dev/null
  note "removed item $item_id"
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
        fieldValues(first:50) { nodes {
          ... on ProjectV2ItemFieldSingleSelectValue {
            name field { ... on ProjectV2SingleSelectField { name } }
          }
        } }
      } }
    }' | jq -r '[.data.node.fieldValues.nodes[]
      | select(.field.name == "Status")][0].name // empty'
}

upsert_issue() {
  local issue=$1 issue_id number title state option_id result item_id prior attempt actual
  issue_id=$(jq -r '.node_id // empty' <<<"$issue")
  number=$(jq -r '.number // empty' <<<"$issue")
  [[ -n $issue_id && -n $number ]] || die 'issue payload lacks node_id or number'
  title=$(jq -r '.milestone.title // empty' <<<"$issue")
  if [[ -z $title ]]; then
    remove_issue "$issue_id"
    return
  fi
  state=$(jq -r --arg label "$progress_label" '
    if .state == "closed" then "Done"
    elif any(.labels[]?; .name == $label) then "In Progress"
    else "Todo" end' <<<"$issue")
  option_id=$(status_option_id "$state")
  prior=$(project_item_for_issue "$issue_id")
  result=$(gh api graphql -f project="$project_id" -f issue="$issue_id" -f query='
    mutation($project:ID!, $issue:ID!) {
      addProjectV2ItemById(input:{projectId:$project, contentId:$issue}) {
        item { id }
      }
    }')
  item_id=$(jq -r '.data.addProjectV2ItemById.item.id // empty' <<<"$result")
  [[ -n $item_id ]] || die "cannot add issue #$number to project"
  # The built-in item-added workflow can land after the mutation and reset Status to Todo.
  [[ -n $prior || $state == Todo ]] || sleep 2
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
  local milestone_lines milestone encoded title number issue_lines issue_encoded issue cursor page stale_ids item_id
  milestone_lines=$(gh api --paginate "repos/$repo/milestones?state=all&per_page=100" \
    --jq '.[] | @base64') || die 'cannot list milestones'
  while IFS= read -r encoded; do
    [[ -n $encoded ]] || continue
    milestone=$(printf '%s' "$encoded" | base64 -d)
    title=$(jq -r '.title' <<<"$milestone")
    number=$(jq -r '.number' <<<"$milestone")
    ensure_view "$title"
    issue_lines=$(gh api --paginate "repos/$repo/issues?state=all&milestone=$number&per_page=100" \
      --jq '.[] | select(.pull_request == null) | @base64') || die "cannot list issues for milestone $number"
    while IFS= read -r issue_encoded; do
      [[ -n $issue_encoded ]] || continue
      issue=$(printf '%s' "$issue_encoded" | base64 -d)
      upsert_issue "$issue"
    done <<<"$issue_lines"
  done <<<"$milestone_lines"

  # Repair items whose milestone was removed while the event workflow was unavailable.
  cursor=''
  stale_ids=''
  while :; do
    if [[ -n $cursor ]]; then
      page=$(gh api graphql -f owner="$owner" -F number="$project_number" -f after="$cursor" -f query='
        query($owner:String!, $number:Int!, $after:String) {
          organization(login:$owner) { projectV2(number:$number) {
            items(first:100, after:$after) { nodes { id content {
              ... on Issue { milestone { title } repository { nameWithOwner } }
            } } pageInfo { hasNextPage endCursor } }
          } }
        }')
    else
      page=$(gh api graphql -f owner="$owner" -F number="$project_number" -f query='
        query($owner:String!, $number:Int!, $after:String) {
          organization(login:$owner) { projectV2(number:$number) {
            items(first:100, after:$after) { nodes { id content {
              ... on Issue { milestone { title } repository { nameWithOwner } }
            } } pageInfo { hasNextPage endCursor } }
          } }
        }')
    fi
    stale_ids+=$(jq -r --arg repo "$repo" '.data.organization.projectV2.items.nodes[]
      | select(.content.repository.nameWithOwner == $repo and .content.milestone == null)
      | .id' <<<"$page")$'\n'
    [[ $(jq -r '.data.organization.projectV2.items.pageInfo.hasNextPage' <<<"$page") == true ]] || break
    cursor=$(jq -r '.data.organization.projectV2.items.pageInfo.endCursor' <<<"$page")
    [[ -n $cursor && $cursor != null ]] || die 'project pagination returned no cursor'
  done
  while IFS= read -r item_id; do
    [[ -n $item_id ]] || continue
    gh api graphql -f project="$project_id" -f item="$item_id" -f query='
      mutation($project:ID!, $item:ID!) {
        deleteProjectV2Item(input:{projectId:$project, itemId:$item}) { deletedItemId }
      }' >/dev/null
    note "removed stale item $item_id"
  done <<<"$stale_ids"
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
    title=$(jq -r '.milestone.title // empty' <<<"$event")
    previous=$(jq -r '.changes.title.from // empty' <<<"$event")
    [[ -n $title ]] || die 'milestone event has no title'
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
