FROM node:20-slim

# Install FFmpeg and clean up apt cache
RUN apt-get update && apt-get install -y ffmpeg && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy dependency definition
COPY package*.json ./

# Install production dependencies
RUN npm install --production

# Copy application files
COPY . .

# Expose ports
EXPOSE 5992
EXPOSE 49000-49100/udp

ENV PORT=5992

CMD ["node", "server.js"]
