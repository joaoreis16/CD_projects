#!/bin/bash

worker="worker""$1"
docker rm worker
docker build --tag projecto_final .
echo 'Built!'
docker run --name worker projecto_final