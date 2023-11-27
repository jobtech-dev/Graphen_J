#!/bin/sh

chmod +x ./hook/pre-commit-hook-install.sh
./hook/pre-commit-hook-install.sh
chmod +x .git/hooks/pre-commit
chmod +x .git/hooks/pre-commit-hook.sh
sbt clean compile

