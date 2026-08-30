#!/bin/bash
# Launches etl-host for the soak, from inside WSL (real Linux, real SIGTERM).
#
# Runs io.quarkus.runner.GeneratedMain directly on a plain classpath instead of
# `java -jar quarkus-run.jar` (io.quarkus.bootstrap.runner.QuarkusEntryPoint). QuarkusEntryPoint
# builds its runtime classpath from a binary index (quarkus/quarkus-application.dat) baked at
# build time, which only lists what `mvn package` resolved - so it does NOT see a jar dropped
# into lib/main afterwards. That matters here because etl-host/pom.xml declares
# com.oracle.database.jdbc:ojdbc11 with <scope>test</scope>: the production quarkus-app has no
# Oracle JDBC driver on it at all. GeneratedMain has no such index; it is an ordinary main()
# resolved via a plain -cp, so a driver jar added to lib/main IS visible. See soak/README.md
# "Findings outside soak/" for the production implication of the scope=test gap this works around.
set -euo pipefail
cd "$(dirname "$0")/../.."   # repo root

APP=soak/run/quarkus-app
CP="$APP/quarkus/generated-bytecode.jar:$APP/quarkus/transformed-bytecode.jar:$APP/app/*:$APP/lib/boot/*:$APP/lib/main/*"

exec /tmp/jdk21/bin/java -cp "$CP" \
  -Dkotlinx.coroutines.debug=on \
  -Detl-host.cache.storage-path=soak/run/state/cache \
  -Detl-host.cache.temp-directory=soak/run/state/tmp \
  -Detl-host.cache.refresh-interval=PT20S \
  -Detl-host.etl.task-directory=soak/run/tasks \
  -Detl-host.etl.scratch-directory=soak/run/state/scratch \
  -Detl-host.etl.target-url=jdbc:duckdb:soak/run/state/report.db \
  -Detl-host.source.url="jdbc:oracle:thin:@localhost:1521/FREEPDB1" \
  -Detl-host.source.username=system \
  -Detl-host.source.password=soakpw \
  io.quarkus.runner.GeneratedMain
