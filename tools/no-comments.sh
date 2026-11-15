#!/usr/bin/env bash
cd "$(dirname "${BASH_SOURCE[0]}")/.." || exit 2
out=$(
  grep -rnP '//|/\*' crates tests --include='*.rs' | grep -vP '^[^:]+:\d+:\s*// allow: \S' | perl -ne '/^([^:]+:\d+:)(.*)/ or next; my $c = $2; $c =~ s/r(#*)".*?"\1|"(?:[^"\\]|\\.)*"|\x27(?:[^\x27\\]|\\.)\x27//g; print if $c =~ m{//|/\*}'
  grep -rlzP '(?m)^[ \t]*// allow: [^\n]*(\n|\z)(?![ \t]*#\[allow\()' crates tests --include='*.rs'
  grep -rnP '#!?\[doc\b' crates tests --include='*.rs'
)
if [ -n "$out" ]; then
  echo "$out"
  echo "NO COMMENTS FAIL"
  exit 1
fi
echo "NO COMMENTS PASS"
