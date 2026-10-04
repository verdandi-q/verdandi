#!/usr/bin/env bash
TOOLS=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ROOT=$(dirname "$TOOLS")
. "$TOOLS/.vhdx-baseline"
VHDX_WSL=/mnt/c/Users/ricky/AppData/Local/Packages/CanonicalGroupLimited.Ubuntu_79rhkp1fndgsc/LocalState/ext4.vhdx
VHDX_DOCKER=/mnt/c/Users/ricky/AppData/Local/Docker/wsl/disk/docker_data.vhdx
VHDX_DDSYS=/mnt/c/Users/ricky/AppData/Local/Docker/wsl/main/ext4.vhdx
GB=1000000000
HOST_CAP=$((50 * GB))
RSS_MAX=$((16 * GB))
C_FLOOR=$((100 * GB))
WRITTEN_CAP=$((200 * GB))
BUSY_CAP=1200
MIN_RUN=120
COOL=120
ROW=${SAFETY_ROW:-none}

num() {
  local v=${!1:-$2}
  if [[ ! $v =~ ^[1-9][0-9]{0,$(( $3 - 1 ))}$ ]]; then
    echo "usage: $1 must be a whole number from 1, of at most $3 digits" >&2
    return 2
  fi
  echo $(( v < $4 ? v : $4 ))
}

SPIKE_CAP=$(num SAFETY_SPIKE_CAP "$HOST_CAP" 15 "$HOST_CAP") || exit 2
RSS_CAP=$(num SAFETY_RSS_CAP "$RSS_MAX" 15 "$RSS_MAX") || exit 2
TIME_LIMIT=$(num SAFETY_TIME_LIMIT "$BUSY_CAP" 9 "$BUSY_CAP") || exit 2
RATE_CAP=$(num SAFETY_RATE_CAP 20000000 9 20000000) || exit 2

gb() { awk -v b="$1" 'BEGIN { printf "%.2f", b / 1e9 }'; }

c_free() { df -B1 --output=avail /mnt/c | tail -1 | tr -d ' '; }

written() { awk '$3 ~ /^sd[a-z]+$/ { s += $10 } END { printf "%d", s * 512 }' /proc/diskstats; }

host_growth() {
  local g=$((BASE_C_FREE - $(c_free))) d
  for d in $(( $(stat -c %s "$VHDX_WSL") - BASE_WSL )) $(( $(stat -c %s "$VHDX_DOCKER") - BASE_DOCKER )) $(( $(stat -c %s "$VHDX_DDSYS") - BASE_DDSYS )); do
    (( d > g )) && g=$d
  done
  echo "$g"
}

spike_usage() {
  if [ -d "$ROOT/target" ]; then du -sb "$ROOT/target" | cut -f1; else echo 0; fi
}

fs_used() { df -B1 --output=used "$ROOT" | tail -1 | tr -d ' '; }

group_rss() { ps -eo pgid=,rss= | awk -v g="$1" '$1 == g { s += $2 } END { printf "%d", s * 1024 }'; }

cooldown() {
  local busy=0 end=0 now
  [ -f "$TOOLS/.last-heavy" ] && read -r busy end <"$TOOLS/.last-heavy"
  now=$(date +%s)
  if (( now - end >= COOL )); then
    busy=0
  elif (( BUSY_CAP - busy < MIN_RUN )); then
    echo "cooldown wait-s=$(( end + COOL - now ))"
    sleep $(( end + COOL - now ))
    busy=0
  fi
  BUSY=$busy
}

stop_cmd() {
  kill -TERM -- "-$PID" 2>/dev/null
  for _ in $(seq 10); do kill -0 -- "-$PID" 2>/dev/null || return 0; sleep 1; done
  kill -KILL -- "-$PID" 2>/dev/null
}

trim() {
  TRIM=skipped
  sudo -n fstrim -v / 2>&1 && TRIM=ok
}

cleanup() {
  trap - EXIT INT TERM
  stop_cmd
  rm -rf "$ROOT/target/selftest" "$ROOT/target/tmp"
  trim
  echo "vhdx wsl=$(stat -c %s "$VHDX_WSL") docker=$(stat -c %s "$VHDX_DOCKER") dd-sys=$(stat -c %s "$VHDX_DDSYS") c-free-gb=$(gb "$(c_free)") host-growth-gb=$(gb "$(host_growth)") spike-gb=$(gb "$(spike_usage)") written-gb=$(gb $(( $(written) - W0 ))) trim=$TRIM row=$ROW"
  echo "$(( BUSY + SECONDS - START )) $(date +%s)" >"$TOOLS/.last-heavy"
  echo done >&"$WATCH"
}

guard() {
  local mode=$1 cf hg sp w rss rate abort="" rc n=0 sp0 u0
  shift
  exec 9>"$TOOLS/.heavy.lock"
  flock 9
  cooldown
  cf=$(c_free) hg=$(host_growth) sp=$(spike_usage)
  if (( cf < C_FLOOR || hg > HOST_CAP || sp > SPIKE_CAP )); then
    echo "ABORTED disk-budget host-growth-gb=$(gb "$hg") spike-gb=$(gb "$sp") c-free-gb=$(gb "$cf") row=$ROW"
    return 3
  fi
  cd "$ROOT" || return 1
  START=$SECONDS W0=$(written) LIMIT=$(( TIME_LIMIT < BUSY_CAP - BUSY ? TIME_LIMIT : BUSY_CAP - BUSY ))
  local -a ts=("$SECONDS") ws=(0) death=()
  command -v setpriv >/dev/null && death=(setpriv --pdeathsig KILL)
  if [ "$mode" = heavy ]; then
    "${death[@]}" taskset -c 8-23 timeout -k 10 "$LIMIT" "$@" 9>&- &
  else
    setsid "${death[@]}" taskset -c 8-23 "$@" 9>&- &
  fi
  PID=$!
  exec {WATCH}> >(exec setsid bash -c 'read -r m; [ "$m" = done ] && exit; kill -KILL -- "-$1"; rm -rf "$2/target/selftest" "$2/target/tmp"; echo "$3 $(date +%s)" >"$2/tools/.last-heavy"' _ "$PID" "$ROOT" "$BUSY_CAP" >/dev/null 2>&1)
  sp0=$sp u0=$(fs_used)
  trap cleanup EXIT
  trap 'abort=signal; stop_cmd' INT TERM
  while kill -0 "$PID" 2>/dev/null; do
    sleep 1
    rss=$(group_rss "$PID")
    if (( rss > RSS_CAP )); then
      echo "ABORTED rss-gb=$(gb "$rss") row=$ROW"
      kill -KILL -- "-$PID" 2>/dev/null
      abort=rss
      break
    fi
    if (( ++n % 5 == 0 )); then
      sp0=$(spike_usage) u0=$(fs_used)
    fi
    cf=$(c_free) hg=$(host_growth) sp=$(( sp0 + $(fs_used) - u0 )) w=$(( $(written) - W0 ))
    (( n % 5 == 0 )) && ts+=("$SECONDS") ws+=("$w")
    if (( cf < C_FLOOR || hg > HOST_CAP || sp > SPIKE_CAP )); then
      echo "ABORTED disk c-free-gb=$(gb "$cf") host-growth-gb=$(gb "$hg") spike-gb=$(gb "$sp") row=$ROW"
      abort=disk
    elif (( w > WRITTEN_CAP )); then
      echo "ABORTED written-gb=$(gb "$w") row=$ROW"
      abort=written
    elif [ "$mode" = long ] && (( n % 5 == 0 && ${#ws[@]} > 12 )); then
      rate=$(( (w - ws[-13]) / (ts[-1] - ts[-13]) ))
      if (( rate > RATE_CAP )); then
        echo "ABORTED write-mbps=$(awk -v r="$rate" 'BEGIN { printf "%.1f", r / 1e6 }') row=$ROW"
        abort=rate
      fi
    fi
    [ -n "$abort" ] && break
  done
  [ -n "$abort" ] && stop_cmd
  wait "$PID"
  rc=$?
  if [ "$mode" = heavy ] && [ -z "$abort" ] && (( SECONDS - START >= LIMIT && (rc == 124 || rc == 137) )); then
    echo "ABORTED reason=time row=$ROW"
  fi
  [ -n "$abort" ] && rc=2
  return "$rc"
}

selftest() {
  local out ok=0 rc w0 w1 wpid busy
  out=$(SAFETY_ROW=selftest SAFETY_SPIKE_CAP=$(( $(spike_usage) + GB )) "$0" long sh -c 'mkdir -p target/selftest && dd if=/dev/zero of=target/selftest/f bs=1M count=2048 conv=fsync && sleep 120' 2>&1)
  echo "$out"
  if grep -qE '^ABORTED disk c-free-gb=[0-9.]+ host-growth-gb=-?[0-9.]+ spike-gb=[0-9.]+ row=selftest$' <<<"$out" && grep -q '^vhdx ' <<<"$out" && [ ! -e "$ROOT/target/selftest" ]; then
    echo "check1 PASS selftest-dir=deleted"
  else
    echo "check1 FAIL"
    ok=1
  fi
  out=$(SAFETY_ROW=selftest SAFETY_TIME_LIMIT=5 "$0" heavy sleep 30 2>&1)
  echo "$out"
  if grep -qx 'ABORTED reason=time row=selftest' <<<"$out"; then echo "check2 PASS"; else echo "check2 FAIL"; ok=1; fi
  w0=$(written)
  SAFETY_ROW=selftest "$0" heavy sh -c 'mkdir -p target/selftest && dd if=/dev/zero of=target/selftest/f bs=1M count=1024 conv=fsync'
  w1=$(written)
  if (( w1 - w0 >= 1024 * 1024 * 1024 )); then
    echo "diskstats PASS written-gb=$(gb $(( w1 - w0 )))"
  else
    echo "diskstats FAIL written-gb=$(gb $(( w1 - w0 )))"
    ok=1
  fi
  out=$(SAFETY_ROW=selftest SAFETY_RSS_CAP=$GB "$0" heavy sh -c '(trap "" TERM; ulimit -v 6000000; exec tail /dev/zero) & sleep 600' 2>&1)
  echo "$out"
  if grep -qE '^ABORTED rss-gb=[0-9.]+ row=selftest$' <<<"$out" && ! pgrep -fx 'tail /dev/zero' >/dev/null && flock -n "$TOOLS/.heavy.lock" true; then
    echo "check4 PASS group=stopped lock=free"
  else
    echo "check4 FAIL"
    ok=1
  fi
  out=$(SAFETY_ROW=selftest "$0" heavy sh -c '(trap "" TERM; exec sleep 77) & sleep 1' 2>&1)
  echo "$out"
  if ! pgrep -fx 'sleep 77' >/dev/null && flock -n "$TOOLS/.heavy.lock" true; then
    echo "check5 PASS leftover=stopped lock=free"
  else
    echo "check5 FAIL"
    ok=1
  fi
  SAFETY_ROW=selftest "$0" heavy sh -c 'mkdir -p target/selftest; (trap "" TERM; exec sleep 78) & sleep 600' >/dev/null 2>&1 &
  wpid=$!
  for _ in $(seq 400); do pgrep -fx 'sleep 78' >/dev/null && [ -d "$ROOT/target/selftest" ] && break; sleep 1; done
  kill -KILL "$wpid"
  wait "$wpid" 2>/dev/null
  busy=$(flock -w 60 "$TOOLS/.heavy.lock" cut -d' ' -f1 "$TOOLS/.last-heavy")
  if ! pgrep -fx 'sleep 78' >/dev/null && [ ! -e "$ROOT/target/selftest" ] && flock -n "$TOOLS/.heavy.lock" true && (( busy == BUSY_CAP )); then
    echo "check6 PASS wrapper-kill leftover=stopped selftest-dir=deleted lock=free busy-s=$busy"
  else
    echo "check6 FAIL busy-s=$busy"
    ok=1
  fi
  out=$(SAFETY_ROW=selftest SAFETY_TIME_LIMIT=0 "$0" heavy true 2>&1)
  rc=$?
  echo "$out"
  if (( rc == 2 )) && grep -qx 'usage: SAFETY_TIME_LIMIT must be a whole number from 1, of at most 9 digits' <<<"$out"; then
    echo "check7 PASS time-limit-0=refused"
  else
    echo "check7 FAIL rc=$rc"
    ok=1
  fi
  out=$(SAFETY_ROW=selftest SAFETY_RATE_CAP=1 "$0" long sh -c 'mkdir -p target/selftest && for _ in $(seq 100); do dd if=/dev/zero of=target/selftest/f bs=1M count=8 conv=fsync status=none; sleep 1; done' 2>&1)
  echo "$out"
  if grep -qE '^cooldown wait-s=[0-9]+$' <<<"$out" && grep -qE '^ABORTED write-mbps=[0-9.]+ row=selftest$' <<<"$out" && [ ! -e "$ROOT/target/selftest" ]; then
    echo "check8 PASS cooldown=waited rate=aborted selftest-dir=deleted"
  else
    echo "check8 FAIL"
    ok=1
  fi
  if (( ok == 0 )); then echo "selftest PASS"; else echo "selftest FAIL"; fi
  return "$ok"
}

if [ "${BASH_SOURCE[0]}" = "$0" ]; then
  case $1 in
    heavy | long) guard "$@" ;;
    selftest) selftest ;;
    *) echo "usage: safety.sh heavy|long <command> | selftest" >&2; exit 2 ;;
  esac
fi
