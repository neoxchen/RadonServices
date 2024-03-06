ECHO "Copying library files..."
cd ..
robocopy ./commons/ ./radon/src/commons/ /MIR
cd ./radon

ECHO "Building image..."
docker build --no-cache -t dockerneoc/radon:pipeline-radon .

ECHO "Publishing to docker hub..."
docker push dockerneoc/radon:pipeline-radon

ECHO "Complete!"
