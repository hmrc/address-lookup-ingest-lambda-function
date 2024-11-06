#!/usr/bin/env groovy
pipeline {
  agent {
      label 'commonagent'
  }
  environment {
    // Use Java 11 (needed after Jenkins upgrade)
    JAVA_HOME = '/usr/lib/jvm/java-11-openjdk-amd64'
    PATH = "/usr/lib/jvm/java-11-openjdk-amd64/bin:$PATH"
    JDK_FILE_NAME = 'openjdk-jre-11.0.17+8.tgz'
    JAVA_VERSION = '11.0.17+8'
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
    stage ('Run cip-attrep-terraform job') {
      steps {
        build(job: 'attrep/terraform/build-all-environments', wait: false)
      }
    }
  }
}