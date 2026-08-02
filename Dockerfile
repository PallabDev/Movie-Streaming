FROM node:20-slim

# Install FFmpeg, Python3 (with pip), and C++ build tools for Mediasoup compilation
RUN apt-get update && apt-get install -y ffmpeg python3 python3-pip build-essential make g++ && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy source code and files
COPY package*.json ./
COPY . .

# Install dependencies and build browser client bundle
RUN npm install
RUN npm run build:client

EXPOSE 5992

ENV PORT=5992

CMD ["node", "server.js"]
