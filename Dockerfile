# Application stage
FROM node:20-alpine
WORKDIR /app
COPY package*.json ./
RUN npm ci --only=production
COPY src ./src
# Create output volume mount point
RUN mkdir -p /app/output && chown -R node:node /app/output
USER node
EXPOSE 8080
CMD ["npm", "start"]
