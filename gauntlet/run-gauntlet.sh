#!/usr/bin/env bash
# 一鍵極限驗證。CI 與本機都跑同一支，沒有第二條路。
set -euo pipefail

echo "==> Gauntlet: clean verify"
echo "    1) Kotlin 零警告編譯 (-Werror)"
echo "    2) Detekt 靜態掃描"
echo "    3) 真實 SQLite / DuckDB 整合測試 + ArchUnit 架構檢查"
echo "    4) JaCoCo 分支覆蓋率門檻 (domain / application >= 85%)"
echo "    5) Pitest 變異測試 (mutation score >= 80)"
echo

mvn clean verify "$@"

echo
echo "==> 報告位置"
echo "    Detekt : target/detekt/detekt.html"
echo "    JaCoCo : target/site/jacoco/index.html"
echo "    Pitest : target/pit-reports/index.html"
echo "    Surefire: target/surefire-reports/"
echo
echo "==> 通過後請更新 EVIDENCE.md，數字一律從上面的報告抄，不准手寫。"
