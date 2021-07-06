# !/bin/bash

rm intermediate*
rm mr-out*
go run -race mrworker.go wc.so
