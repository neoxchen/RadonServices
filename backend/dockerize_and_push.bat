ECHO "Copying library files..."
cd ..
robocopy ./commons/ ./backend/src/commons/ /MIR
cd ./backend

ECHO "Building image..."
docker build --no-cache -t dockerneoc/radon:orchestrator .

ECHO "Cleaning up build files..."
rmdir /s /q "./src/commons"

ECHO "Publishing to docker hub..."
docker push dockerneoc/radon:orchestrator

ECHO "Complete!"
