FROM node:22-alpine
WORKDIR /app
# Build toolchain for better-sqlite3's native addon. Alpine uses musl, so the
# package's prebuilt (glibc) binaries don't apply and it compiles from source.
RUN apk add --no-cache --virtual .build-deps python3 make g++
COPY package.json ./
RUN npm install --production && apk del .build-deps
COPY . .

# Persistent store for historical metrics + anomaly baselines. Declared as a
# volume so the better-sqlite3 DB survives container restarts; if the volume is
# absent or unwritable the metrics store degrades to in-memory (persistent:false).
ENV ECHO_DATA_DIR=/app/data
RUN mkdir -p /app/data
VOLUME ["/app/data"]

EXPOSE 3000
# 127.0.0.1, not localhost: in Alpine `/etc/hosts` lists `::1 localhost`
# before `127.0.0.1 localhost`, BusyBox wget resolves to the v6 address
# first, and Express's app.listen(PORT, '0.0.0.0') binds IPv4 only — so
# wget against `localhost` fails with "connection refused" and the
# container shows up as `unhealthy` even when the dapp is fine.
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
  CMD wget -qO- http://127.0.0.1:3000/health || exit 1
CMD ["node", "server.js"]
