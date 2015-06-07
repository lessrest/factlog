FROM node:latest
RUN npm install -g redis
ENV NODE_PATH /usr/local/lib/node_modules
COPY index.js /lib/index.js
CMD node /lib/index.js
