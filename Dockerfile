# syntax=docker/dockerfile:1

# ---- build ----
# Mesma versão de Go do go.mod (1.26).
FROM golang:1.26 AS build
WORKDIR /src

# Dependências primeiro: a camada fica em cache enquanto go.mod/go.sum não mudam.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/app ./cmd/app

# ---- runtime ----
# Imagem mínima, sem shell, rodando como usuário sem privilégios.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/app /app
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/app"]
