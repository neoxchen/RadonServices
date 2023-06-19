ECHO "Building image..."
docker build --no-cache -t dockerneoc/radon:pipeline-fetch .

ECHO "Publishing to docker hub..."
docker push dockerneoc/radon:pipeline-fetch

ECHO "Complete!"
