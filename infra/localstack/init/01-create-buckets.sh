#!/bin/bash
set -e

awslocal s3 mb s3://techflow-bronze 2>/dev/null || true
awslocal s3 mb s3://techflow-silver 2>/dev/null || true
awslocal s3 mb s3://techflow-gold 2>/dev/null || true

echo "Buckets Bronze, Silver e Gold preparados."