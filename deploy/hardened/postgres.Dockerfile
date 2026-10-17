FROM postgres:16-alpine
# Runtime always uses UID 70; the root-only privilege-switch helper is unnecessary.
RUN apk upgrade --no-cache && rm -f /usr/local/bin/gosu
USER 70:70
