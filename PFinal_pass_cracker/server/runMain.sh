#!/bin/bash

docker stop server
docker rm server
docker run --name server diogogomes/cd2021:0.1