FROM debian:bookworm-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    cmake \
    git \
    python3 \
    libsdl2-dev \
    liblua5.4-dev \
    libpng-dev \
    zlib1g-dev \
    libzip-dev \
    zipcmp \
    zipmerge \
    ziptool \
    libsqlite3-dev \
    libelf-dev \
    && rm -rf /var/lib/apt/lists/*

# Build mGBA from source with Lua scripting enabled
RUN git clone --depth 1 https://github.com/mgba-emu/mgba.git /mgba

COPY patch_mgba_sdl.py /tmp/patch_mgba_sdl.py
RUN python3 /tmp/patch_mgba_sdl.py
RUN grep -q 'parseLongScriptArg' /mgba/src/platform/sdl/main.c || \
    (echo "FATAL: Source patch failed — mGBA source has changed" && exit 1)

WORKDIR /mgba/build
RUN cmake .. \
    -DCMAKE_BUILD_TYPE=Release \
    -DBUILD_SDL=ON \
    -DBUILD_QT=OFF \
    -DBUILD_LIBRETRO=OFF \
    -DUSE_FFMPEG=OFF \
    -DUSE_MINIZIP=OFF \
    -DUSE_DISCORD_RPC=OFF \
    -DUSE_EDITLINE=OFF \
    -DUSE_ELF=OFF \
    -DUSE_SQLITE3=OFF \
    -DUSE_LUA=ON \
    && make -j$(nproc) mgba-sdl \
    && make install DESTDIR=/install

FROM debian:bookworm-slim

# Runtime dependencies only
RUN apt-get update && apt-get install -y --no-install-recommends \
    xvfb \
    x11-utils \
    tini \
    procps \
    libsdl2-2.0-0 \
    libgl1 \
    liblua5.4-0 \
    libpng16-16 \
    zlib1g \
    libzip4 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /install/usr/local/bin/mgba /usr/local/bin/mgba-sdl
COPY --from=builder /install/usr/local/lib/libmgba.so* /usr/local/lib/
COPY scripts/mGBASocketServer.lua /app/mGBASocketServer.lua
COPY entrypoint.sh /entrypoint.sh
RUN ldconfig && chmod +x /entrypoint.sh

EXPOSE 8888

ENTRYPOINT ["/usr/bin/tini", "--", "/entrypoint.sh"]
