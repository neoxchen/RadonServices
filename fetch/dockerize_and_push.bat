ECHO "Copying library files..."
cd ..
robocopy ./commons/ ./fetch/src/commons/ /MIR
cd ./fetch

ECHO "Building image..."
docker build --no-cache -t dockerneoc/radon:pipeline-fetch .

ECHO "Cleaning up build files..."
rmdir /s /q "./src/commons"

ECHO "Publishing to docker hub..."
docker push dockerneoc/radon:pipeline-fetch

ECHO "Complete!"
