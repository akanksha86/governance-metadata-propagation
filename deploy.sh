#!/bin/bash

# Stop on first error
set -e

# Configuration
PROJECT_ID=$(gcloud config get-value project)
SERVICE_NAME="governance-steward"
REGION="europe-west1"
REPO_NAME="governance-repo"
IMAGE_NAME="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO_NAME}/${SERVICE_NAME}:latest"

echo "🚀 Starting deployment of ${SERVICE_NAME} to ${REGION}..."

# 1. Ensure Artifact Registry repository exists
echo "🔍 Checking for Artifact Registry repository..."
if ! gcloud artifacts repositories describe ${REPO_NAME} --location=${REGION} >/dev/null 2>&1; then
  echo "🆕 Creating repository ${REPO_NAME} in ${REGION}..."
  gcloud artifacts repositories create ${REPO_NAME} \
    --repository-format=docker \
    --location=${REGION} \
    --description="Repository for Governance Steward images"
fi

# 2. Configure Docker for Artifact Registry
echo "🔐 Configuring Docker for Artifact Registry..."
gcloud auth configure-docker ${REGION}-docker.pkg.dev --quiet

# 3. Build the Docker image for the correct platform (Cloud Run requires amd64)
echo "📦 Building Docker image for linux/amd64..."
docker build --platform linux/amd64 -t ${IMAGE_NAME} .

# 4. Push the image to Artifact Registry
echo "📤 Pushing image to Artifact Registry..."
docker push ${IMAGE_NAME}

# 5. Extract env vars from .env for Cloud Run (excluding comments and empty lines)
echo "📝 Preparing environment variables..."
ENV_VARS=$(grep -v '^#' .env | grep -v '^\s*$' | tr '\n' ',' | sed 's/,$//')

# 6. Deploy to Cloud Run
echo "☸️ Deploying to Cloud Run..."
gcloud run deploy ${SERVICE_NAME} \
  --image ${IMAGE_NAME} \
  --platform managed \
  --region ${REGION} \
  --port 7860 \
  --set-env-vars="${ENV_VARS}"

# 7. Attempt to allow unauthenticated access (may fail due to IAM restrictions)
echo "🔓 Attempting to enable public access..."
gcloud run services add-iam-policy-binding ${SERVICE_NAME} \
  --region=${REGION} \
  --member="allUsers" \
  --role="roles/run.invoker" || echo "⚠️ Warning: Failed to set IAM policy. You may need to manually enable 'Allow unauthenticated' in the GCP Console."

echo "✅ Deployment process finished!"
gcloud run services describe ${SERVICE_NAME} --region ${REGION} --format 'value(status.url)'
