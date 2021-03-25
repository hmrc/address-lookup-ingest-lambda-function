#!/usr/bin/env groovy
pipeline {
  agent {
      label 'commonagent'
  }

  stages {
    stage('Build artefact') {
      steps {
        ansiColor('xterm') {
        	sh("""
               docker build --no-cache --tag=address-lookup-ingest-lambda-builder-image .
               docker run -rm -t -v \$(pwd):/work --name=build-address-lookup-ingest-lambda address-lookup-ingest-lambda-builder-image
               """)
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
    stage ('Run cip-attval-terraform job') {
      steps {
        build job: 'cip-attval-terraform/terraform-environments'
      }
    }
  }
}
