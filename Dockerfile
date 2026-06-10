FROM node:22-alpine AS builder
WORKDIR /build

ARG PACKAGES_READ_TOKEN
RUN echo "@rebus-industries:registry=https://npm.pkg.github.com" >> /root/.npmrc && \
    echo "//npm.pkg.github.com/:_authToken=${PACKAGES_READ_TOKEN}" >> /root/.npmrc

COPY package.json package-lock.json* ./
RUN npm ci --no-audit --no-fund

COPY tsconfig.json ./
COPY src ./src
RUN npm run build

FROM node:22-alpine AS runtime
WORKDIR /prism-agent

ARG PACKAGES_READ_TOKEN
RUN echo "@rebus-industries:registry=https://npm.pkg.github.com" >> /root/.npmrc && \
    echo "//npm.pkg.github.com/:_authToken=${PACKAGES_READ_TOKEN}" >> /root/.npmrc

COPY package.json package-lock.json* ./
RUN npm ci --omit=dev --no-audit --no-fund && npm cache clean --force && rm /root/.npmrc

COPY --from=builder /build/dist ./dist

ENV NODE_ENV=production
ENV PORT=8767
ENV UPLOAD_DIR=/var/lib/prism/uploads

EXPOSE 8767
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD wget -qO- http://localhost:${PORT}/health || exit 1

CMD ["node", "dist/main.js"]
