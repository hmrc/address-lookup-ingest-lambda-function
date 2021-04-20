#!/usr/bin/env groovy
pipeline {
  agent {
      label 'commonagent'
  }

  stages {
    stage('Build artefact') {
      steps {
        ansiColor('xterm') {
        	sh('make build')
        }
      }
    }
    stage('Upload to s3') {
      steps {
        sh("""
           make push-s3 S3_BUCKET=txm-lambda-functions-integration
           make push-s3 S3_BUCKET=txm-lambda-functions-qa
           make push-s3 S3_BUCKET=txm-lambda-functions-staging
           make push-s3 S3_BUCKET=txm-lambda-functions-production
           """)
      }
    }
  }
}
