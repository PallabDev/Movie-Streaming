FROM node:20-slim

# Install FFmpeg, Python3 (with pip), and C++ build tools for Mediasoup compilation
RUN apt-get update && apt-get install -y ffmpeg python3 python3-pip build-essential make g++ && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy source code and files
COPY package*.json ./
COPY . .

# Create public/js and build browser client bundle
RUN mkdir -p public/js
RUN npm install
RUN npx esbuild node_modules/mediasoup-client/lib/index.js --bundle --minify --format=iife --global-name=mediasoupClient "--footer:js=if(typeof window !== 'undefined') window.mediasoupClient = mediasoupClient; if(typeof globalThis !== 'undefined') globalThis.mediasoupClient = mediasoupClient;" --outfile=public/js/mediasoup-client.min.js

EXPOSE 5992

ENV PORT=5992

CMD ["node", "server.js"]
