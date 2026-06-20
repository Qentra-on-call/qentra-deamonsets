# Qentra log agent — tiny, zero-dependency. Distroless Node for a small,
# shell-less, non-root image (~a few MB of app on the base).
FROM gcr.io/distroless/nodejs20-debian12:nonroot
WORKDIR /app
COPY index.js package.json ./
USER nonroot
EXPOSE 8080
CMD ["index.js"]
