#!/usr/bin/env bash
set -u  

dir="$(mktemp -d /tmp/symlink-depth.XXXXXX)" || exit 1 
trap 'rm -rf "$dir"' EXIT     # автоочистка

cd "$dir" || exit 1 
printf 'x' > a     

prev="a"  # для следующего симлинка
depth=0  # текущая глубина

while :; do
  depth=$((depth+1))
  cur="$(printf 's%03d' "$depth")"
  ln -s "$prev" "$cur" || exit 1  # создаём cur - prev

  # пробуем открыть через редирект 
  if err=$({ : <"$cur"; } 2>&1); then
    prev="$cur"  # если получилось то идём дальше
  else
    echo "рабочая директория: $dir"
    echo "последняя успешная глубина (open() сработал): $((depth-1))"
    echo "первая неуспешная попытка открытия на глубине: $depth"
    echo "сообщение об ошибке open(): $err"
    exit 0
  fi
done
