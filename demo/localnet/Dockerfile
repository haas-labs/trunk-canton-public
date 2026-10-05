# Canton open-source, as released on github.com/digital-asset/canton. The jar targets Java 17;
# the release itself is built with JDK 21. Pin the version to the one the target network runs.
ARG CANTON_VERSION=3.5.16
FROM eclipse-temurin:21-jre
ARG CANTON_VERSION
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates && rm -rf /var/lib/apt/lists/* \
 && curl -sSL -o /tmp/canton.tar.gz "https://github.com/digital-asset/canton/releases/download/v${CANTON_VERSION}/canton-open-source-${CANTON_VERSION}.tar.gz" \
 && mkdir -p /canton && tar -xzf /tmp/canton.tar.gz -C /canton --strip-components=1 && rm /tmp/canton.tar.gz
WORKDIR /canton
ENTRYPOINT ["/canton/bin/canton"]
