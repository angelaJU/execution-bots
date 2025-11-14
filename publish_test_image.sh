#!/bin/bash

# Set Docker env variables
REGISTRY='260690620822.dkr.ecr.ap-southeast-1.amazonaws.com'
IMAGE='aplbot'
CONTAINER="altonomy/$IMAGE"
VERSION="1.0.9"
TAG='test'

# Docker Build
docker build -t $IMAGE .

# Push Docker to Registry
aws ecr get-login-password --region ap-southeast-1 | docker login --username AWS --password-stdin $REGISTRY

docker tag $IMAGE "$REGISTRY/$CONTAINER:$VERSION"
docker tag $IMAGE "$REGISTRY/$CONTAINER:$TAG"

docker push "$REGISTRY/$CONTAINER:$VERSION"
docker push "$REGISTRY/$CONTAINER:$TAG"
