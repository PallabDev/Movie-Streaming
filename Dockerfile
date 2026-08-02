FROM node:20-slim

# Install FFmpeg, Python3, and C++ build tools for Mediasoup compilation
RUN apt-get update && apt-get install -y ffmpeg python3 build-essential make g++ && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy dependency definition
COPY package*.json ./

# Install dependencies and build client bundle
RUN npm install
RUN npm run build:client

# Copy application files
COPY . .

# Expose port
EXPOSE 5992

ENV PORT=5992

CMD ["node", "server.js"]
