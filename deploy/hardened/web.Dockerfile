FROM node:22-alpine AS build
WORKDIR /build
COPY frontend/package*.json ./
RUN npm ci
COPY frontend/ ./
ENV VITE_API_BASE_URL=/api/v1
RUN npm run build
FROM alpine:3.23
RUN apk add --no-cache nginx curl
COPY --from=build /build/dist /usr/share/nginx/html
COPY deploy/hardened/nginx.conf /etc/nginx/nginx.conf
USER 101:101
ENTRYPOINT ["nginx", "-e", "/dev/stderr", "-g", "daemon off;"]
