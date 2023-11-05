ECHO "Building image..."
docker build --no-cache -t dockerneoc/radon:pipeline-augment .

ECHO "Publishing to docker hub..."
docker push dockerneoc/radon:pipeline-augment

ECHO "Complete!"
