#!/usr/bin/env bash
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
P=$ROOT/plans
N=0
FAIL=0

check() {
  local file=$1 want=$2 text=$3 got
  got=$(tr '\n' ' ' <"$P/$file" | tr -s ' ' | grep -oF -- "$text" | wc -l)
  N=$((N + 1))
  if [ "$got" -ne "$want" ]; then
    echo "FAIL $file want=$want got=$got: $text"
    FAIL=$((FAIL + 1))
  fi
}

check IMPLEMENTATION.md 1 'sends `left` with `remaining`'
check IMPLEMENTATION.md 1 'at most 5 copies (`MAX_COPIES`)'
check ARCHITECTURE.md 1 'It is at most 5 copies:'
check ARCHITECTURE.md 1 'at most 5 copies in all.'
check ARCHITECTURE.md 1 '| 6 nodes over 3 zones, rare losses | 3, one per zone | 3 to 4 |'
check IMPLEMENTATION.md 1 'the ack count is never below 2 (`KEYED_MIN_COPIES`, I22)'
check ARCHITECTURE.md 1 'the ack count is never below 2 (`KEYED_MIN_COPIES`, I22)'
check IMPLEMENTATION.md 1 'each trusted only after a sealed reply. A client `Host` is never a source of it.'
check ARCHITECTURE.md 1 'each trusted only after a sealed reply. A client `Host` is never a source of that address.'
check IMPLEMENTATION.md 1 "(Changed in M2, assumption 33, pending Ricky's review.)"
check ARCHITECTURE.md 1 "(Changed in M2, assumption 33, pending Ricky's review.)"
check 07-http-api.md 1 "(assumption 33, pending Ricky's review)"
check ARCHITECTURE.md 1 ', `maximumPercent: 100` and `minimumHealthyPercent`'
check ARCHITECTURE.md 1 'rolling updates that stop one instance, then start its replacement'
check ARCHITECTURE.md 1 'the `RollingUpdate` strategy with `maxUnavailable: 1`'

if [ "$FAIL" -ne 0 ]; then
  echo "DOC CHECKS FAIL failed=$FAIL of=$N"
  exit 1
fi
echo "DOC CHECKS PASS checks=$N"
