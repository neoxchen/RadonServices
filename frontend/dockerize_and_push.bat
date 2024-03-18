ECHO "Copying library files..."
cd ..
robocopy ./commons/ ./frontend/src/commons/ /MIR
cd ./frontend

ECHO "Building image..."
docker build --no-cache -t dockerneoc/radon:web-interface .

ECHO "Cleaning up build files..."
rmdir /s /q "./src/commons"

ECHO "Publishing to docker hub..."
docker push dockerneoc/radon:web-interface

ECHO "Complete!"
