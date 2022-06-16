#!/bin/sh
find ../stdb -name "*.h" -or -name "*.cc" | xargs cat | wc  -l
