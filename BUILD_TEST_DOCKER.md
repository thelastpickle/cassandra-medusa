# Build and Test Local Docker Image

Quick guide to build a local Docker image with your memory leak fix and test it.

## 🐳 Build Local Image

### Option 1: Simple Build (Recommended for quick testing)

```bash
# From the project root
docker build -f k8s/Dockerfile -t medusa-memory-leak-fix:local .

# If you get a "CPU does not support x86-64-v2" error, use the Ubuntu Dockerfile:
docker build -f k8s/Dockerfile.ubuntu -t medusa-memory-leak-fix:local .

# Or with a more descriptive tag
docker build -f k8s/Dockerfile.ubuntu -t medusa-memory-leak-fix:$(git rev-parse --short HEAD) .
```

### Option 2: Build with Specific Architecture

```bash
# For linux/amd64 (x86_64)
docker build -f k8s/Dockerfile --platform linux/amd64 -t medusa-memory-leak-fix:local .

# For linux/arm64 (Apple Silicon, etc.)
docker build -f k8s/Dockerfile --platform linux/arm64 -t medusa-memory-leak-fix:local .
```

## 🧪 Test with Environment Variables

You can test the fix with aggressive parameters without modifying the code:

### Test 1: Fast Cleanup (5 minutes retention)

```bash
# Use the provided test config file
docker run -it --rm \
  -e MEDUSA_MODE=GRPC \
  -e MEDUSA_BACKUP_RETENTION_SECONDS=300 \
  -e MEDUSA_MAX_COMPLETED_BACKUPS=3 \
  -e MEDUSA_MEMORY_CLEANUP_INTERVAL_SECONDS=600 \
  -e MEDUSA_FORCE_GC_AFTER_BACKUP=true \
  -v $(pwd)/test-medusa.ini:/etc/medusa/medusa.ini:ro \
  medusa-memory-leak-fix:local

# Or with your own config file
docker run -it --rm \
  -e MEDUSA_MODE=GRPC \
  -e MEDUSA_BACKUP_RETENTION_SECONDS=300 \
  -e MEDUSA_MAX_COMPLETED_BACKUPS=3 \
  -e MEDUSA_MEMORY_CLEANUP_INTERVAL_SECONDS=600 \
  -e MEDUSA_FORCE_GC_AFTER_BACKUP=true \
  -v /path/to/your/medusa.ini:/etc/medusa/medusa.ini:ro \
  medusa-memory-leak-fix:local
```

### Test 2: Very Aggressive Cleanup (1 minute retention)

```bash
docker run -it --rm \
  -e MEDUSA_MODE=GRPC \
  -e MEDUSA_BACKUP_RETENTION_SECONDS=60 \
  -e MEDUSA_MAX_COMPLETED_BACKUPS=2 \
  -e MEDUSA_MEMORY_CLEANUP_INTERVAL_SECONDS=120 \
  -e MEDUSA_FORCE_GC_AFTER_BACKUP=true \
  -v $(pwd)/test-medusa.ini:/etc/medusa/medusa.ini:ro \
  medusa-memory-leak-fix:local
```

### Test 3: Disable Forced GC (for comparison)

```bash
docker run -it --rm \
  -e MEDUSA_MODE=GRPC \
  -e MEDUSA_FORCE_GC_AFTER_BACKUP=false \
  -v $(pwd)/test-medusa.ini:/etc/medusa/medusa.ini:ro \
  medusa-memory-leak-fix:local
```

## 📊 Monitoring Logs

To see cleanup logs in real-time:

```bash
docker run -it --rm \
  -e MEDUSA_MODE=GRPC \
  -e MEDUSA_BACKUP_RETENTION_SECONDS=300 \
  -e MEDUSA_MEMORY_CLEANUP_INTERVAL_SECONDS=600 \
  -v $(pwd)/test-medusa.ini:/etc/medusa/medusa.ini:ro \
  medusa-memory-leak-fix:local 2>&1 | grep -i "cleanup\|memory\|GC"
```

## 🔍 Verify the Fix is Present

To verify that your code is in the image:

```bash
# Check the code in the image
docker run --rm medusa-memory-leak-fix:local \
  python3 -c "from medusa.backup_manager import BackupMan; print(f'MAX_COMPLETED_BACKUPS={BackupMan.MAX_COMPLETED_BACKUPS}'); print(f'BACKUP_RETENTION_SECONDS={BackupMan.BACKUP_RETENTION_SECONDS}')"

# Verify supported environment variables
docker run --rm \
  -e MEDUSA_BACKUP_RETENTION_SECONDS=999 \
  -e MEDUSA_MAX_COMPLETED_BACKUPS=5 \
  medusa-memory-leak-fix:local \
  python3 -c "from medusa.backup_manager import BackupMan; print(f'MAX_COMPLETED_BACKUPS={BackupMan.MAX_COMPLETED_BACKUPS}'); print(f'BACKUP_RETENTION_SECONDS={BackupMan.BACKUP_RETENTION_SECONDS}')"
```

## 🚀 Usage in Kubernetes

To test in Kubernetes with your local image, you have several options:

### Option A: Load into Kind/Minikube

```bash
# If using Kind
kind load docker-image medusa-memory-leak-fix:local

# If using Minikube
minikube image load medusa-memory-leak-fix:local
```

### Option B: Push to Private Registry

```bash
# Tag for your registry
docker tag medusa-memory-leak-fix:local your-registry/medusa-memory-leak-fix:test

# Push
docker push your-registry/medusa-memory-leak-fix:test
```

### Option C: ConfigMap for Environment Variables

Create a ConfigMap to test with aggressive parameters:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: medusa-memory-cleanup-test
  namespace: your-namespace
data:
  MEDUSA_BACKUP_RETENTION_SECONDS: "300"  # 5 minutes
  MEDUSA_MAX_COMPLETED_BACKUPS: "3"
  MEDUSA_MEMORY_CLEANUP_INTERVAL_SECONDS: "600"  # 10 minutes
  MEDUSA_FORCE_GC_AFTER_BACKUP: "true"
```

Then in your Deployment:

```yaml
spec:
  template:
    spec:
      containers:
      - name: medusa
        image: medusa-memory-leak-fix:local  # or your registry
        envFrom:
        - configMapRef:
            name: medusa-memory-cleanup-test
```

## 📝 Test Checklist

- [ ] Image built without errors
- [ ] Container starts correctly with `MEDUSA_MODE=GRPC`
- [ ] Environment variables are taken into account
- [ ] Logs show "Running periodic memory cleanup..."
- [ ] Completed backups are cleaned up after the delay
- [ ] IN_PROGRESS backups are never deleted
- [ ] Memory remains stable or decreases

## 🐛 Troubleshooting

### Image won't build
```bash
# Verify you're at the project root
pwd  # should be /home/wart/medusa/cassandra-medusa

# Verify the Dockerfile exists
ls -la k8s/Dockerfile
```

### Environment variables don't work
```bash
# Verify the code uses os.environ.get()
docker run --rm medusa-memory-leak-fix:local \
  python3 -c "import os; print(os.environ.get('MEDUSA_BACKUP_RETENTION_SECONDS', 'NOT_SET'))"
```

### Cleanup doesn't trigger
- Check logs for errors
- Verify that `MEDUSA_MEMORY_CLEANUP_INTERVAL_SECONDS` is not too high
- Verify that backups are actually completed (SUCCESS or FAILED)
