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

# Expose port
EXPOSE 5992

ENV PORT=5992

CMD ["node", "server.js"]
