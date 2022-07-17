#!/bin/bash
# Remember to build your handler executable for Linux!
GOOS=linux GOARCH=amd64 go build -o main lambda_function.go
GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o main_mini lambda_function.go # exclude debug info
rm main.zip
rm main_mini.zip
zip main.zip main
zip main_mini.zip main_mini
rm main
rm main_mini